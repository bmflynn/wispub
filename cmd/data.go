package cmd

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"text/template"

	"github.com/eclipse/paho.golang/paho"
	"github.com/spf13/cobra"
	"gitlab.ssec.wisc.edu/dbrtn/wispub/internal"
)

var dataCmd = &cobra.Command{
	Use:     "data",
	Aliases: []string{"d", "dat"},
	Short:   "Publish a data notification message",
	Long: `Publish a data notification message to a WIS 2.0 MQTT broker.

A data notification message can be sent to notify subscribers of the availability 
of an individual data product file.

See: https://community.wmo.int/activity-areas/wis/wis2-implementation
`,
	Example: `
export WISPUB_BROKER_USER=<username>
export WISPUB_BROKER_PASSWD=<password>

wispub data \
	--broker=ssl://<broker host> \
	--topic=<message topic> \
	--download-url=<product download url> \
	--input=<product file> \
	--datetime=<yyyy-mm-dd>T<hh:mm:ss>Z,<yyyy-mm-dd>T<hh:mm:ss>Z
	--meta-id=<metadata identifier> 

Add the --dryrun flag to print out message information without sending.

More information on topic hierarchy is available here: 
	https://github.com/wmo-im/wis2-topic-hierarchy
`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := cmd.Flags()
		verbose, err := flags.GetBool("verbose")
		cobra.CheckErr(err)
		dryrun, err := flags.GetBool("dryrun")
		cobra.CheckErr(err)

		broker, err := flags.GetString("broker")
		cobra.CheckErr(err)
		brokerURL, err := url.Parse(broker)
		if err != nil {
			return fmt.Errorf("invalid download URL")
		}

		download, err := flags.GetString("download-url")
		cobra.CheckErr(err)
		downloadURL, err := url.Parse(download)
		if err != nil {
			return fmt.Errorf("invalid download URL")
		}

		satellite, err := flags.GetString("satellite")
		cobra.CheckErr(err)

		observation, err := flags.GetString("observation")
		cobra.CheckErr(err)

		center, err := flags.GetString("center")
		cobra.CheckErr(err)

		topic, err := flags.GetString("topic")
		cobra.CheckErr(err)
		topicTmpl, err := template.New("").Parse(topic)
		if err != nil {
			return fmt.Errorf("invalid topic template: %w", err)
		}
		buf := &bytes.Buffer{}
		err = topicTmpl.Execute(buf, struct {
			Satellite, Observation, Center string
		}{satellite, observation, center})
		if err != nil {
			return fmt.Errorf("could not render topic template")
		}
		topic = buf.String()

		input, err := flags.GetString("input")
		cobra.CheckErr(err)

		mimeType, err := flags.GetString("mime-type")
		cobra.CheckErr(err)

		metaId, err := flags.GetString("meta-id")
		cobra.CheckErr(err)

		datetime, err := flags.GetString("datetime")
		cobra.CheckErr(err)

		insecure, err := flags.GetBool("insecure")
		cobra.CheckErr(err)

		setDefaultPort(brokerURL)

		ctx := exitHandlerContext()

		doDataCmd(ctx, brokerURL, downloadURL, input, topic, mimeType, metaId, center, datetime, verbose, dryrun, insecure)
		return nil
	},
}

func init() {
	flags := dataCmd.Flags()
	flags.Bool("verbose", false, "Verbose logging")
	flags.Bool("dryrun", false, "Generate and print the message and topic, but don't send")

	flags.String("broker", "",
		"MQTT broker URL to publish messages to. Can be tcp:// or ssl://. If the port is not included it "+
			"will default to "+fmt.Sprintf("%v for tcp and %v for ssl.", defaultPort, defaultSSLPort))
	flags.StringP("input", "i", "", "Path to the file to send")
	flags.StringP("download-url", "u", "", "Publicly available URL where the data can be downloaded")
	flags.StringP("topic", "t", "", "Topic (template) to publish the message to. Can include template variables: {{.Satellite}}, {{.Observation}}, {{.Center}}")
	flags.Bool("insecure", false, "If using TLS, don't verify the remote server certificate")
	flags.StringP("mime-type", "m", "", "Mime-type for the provided input. If not provided it will be determined by file extension.")
	flags.StringP("data-domain", "d", "DBNet", "Data domain indicator to add to the message properties.dataDomain")
	flags.StringP("datetime", "D", "",
		"Time and date of the data as either a single timestamp or as a comma separated start and end. The format for "+
			"the timestamp(s) is RFC3339, e.g., <yyyy-mm-dd>T<hh:mm:ss>Z")
	flags.StringP("meta-id", "e", "", "Previously registered metadata identifier for data product")

	cobra.CheckErr(cobra.MarkFlagRequired(flags, "broker"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "download-url"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "topic"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "input"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "meta-id"))

	rootCmd.AddCommand(dataCmd)
}

func doDataCmd(
	ctx context.Context,
	brokerURL, downloadURL *url.URL,
	input, topic, mimeType, metaId, center, datetime string,
	verbose, dryrun, insecure bool,
) {
	if verbose {
		log.Printf("connecting to %+s", brokerURL)
	}

	start, end, err := parseDatetime(datetime)
	if err != nil {
		log.Fatalf("failed to parse timestamps: %s", err)
	}

	wisMsg, err := internal.NewNotificationMessage(input, topic, downloadURL, mimeType, metaId, start, end)
	if err != nil {
		log.Fatalf("failed to construct message from input: %s", err)
	}

	var properties map[string]any
	body, err := internal.EncodeMessage(wisMsg, properties)
	if err != nil {
		log.Fatalf("failed to encode message as json: %s", err)
	}

	if dryrun {
		os.Stderr.WriteString(topic + "\n")
		os.Stdout.Write(body)
		os.Stdout.WriteString("\n")
		return
	}

	client, err := internal.NewClient(ctx, brokerURL, strings.ToLower(center), "", insecure)
	if err != nil {
		log.Fatalf("failed to create broker: %s", err)
	}
	defer func() {
		if client == nil {
			return
		}
		err := client.Disconnect(&paho.Disconnect{
			ReasonCode: 0,
		})
		if err != nil {
			log.Printf("unclean disconnect: %s", err)
		}
	}()

	log.Printf("publishing message to topic %s", topic)
	msg := &paho.Publish{
		QoS:   1,
		Topic: topic,
		Properties: &paho.PublishProperties{
			ContentType: "application/json",
		},
		Payload: body,
	}
	if verbose {
		log.Printf("publishing %s", string(body))
	}
	zult, err := client.Publish(ctx, msg)
	if err != nil {
		log.Fatalf("publishing failed: %s", err)
	}
	if zult.ReasonCode != 0 {
		log.Printf("unexpected publish response [code=%v]: %v", zult.ReasonCode, internal.PubReason(zult.ReasonCode))
	}
}
