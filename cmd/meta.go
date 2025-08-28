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

var metaCmd = &cobra.Command{
	Use:     "metadata",
	Aliases: []string{"meta", "m"},
	Short:   "Publish a metadata notification message",
	Long: `Publish a metadata notification message to a WIS 2.0 MQTT broker.

A metadata notification is required for to register a new data stream.

See https://wmo-im.github.io/wcmp2/standard/wcmp2-STABLE.html
`,
	Example: `
export WISPUB_BROKER_USER=<username>
export WISPUB_BROKER_PASSWD=<password>

wispub data \
	--broker=ssl://<broker host> \
	--center=<message topic> \
	--input=<path to metadata JSON file> 

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

		topic, err := flags.GetString("topic")
		cobra.CheckErr(err)
		topicTmpl, err := template.New("").Parse(topic)
		if err != nil {
			return fmt.Errorf("invalid topic template: %w", err)
		}

		center, err := flags.GetString("center")
		cobra.CheckErr(err)

		cobra.CheckErr(err)

		buf := &bytes.Buffer{}
		err = topicTmpl.Execute(buf, struct {
			Center string
		}{center})
		if err != nil {
			return fmt.Errorf("could not render topic template")
		}
		topic = buf.String()

		input, err := flags.GetString("input")
		cobra.CheckErr(err)

		insecure, err := flags.GetBool("insecure")
		cobra.CheckErr(err)

		setDefaultPort(brokerURL)

		ctx := exitHandlerContext()

		doMetaCmd(ctx, brokerURL, input, topic, center, verbose, dryrun, insecure)
		return nil
	},
}

func init() {
	flags := metaCmd.Flags()
	flags.Bool("verbose", false, "Verbose logging")
	flags.Bool("dryrun", false, "Generate and print the message and topic, but don't send")

	flags.String("broker", "",
		"MQTT broker URL to publish messages to. Can be tcp:// or ssl://. If the port is not included it "+
			"will default to "+fmt.Sprintf("%v for tcp and %v for ssl.", defaultPort, defaultSSLPort))
	flags.StringP("input", "i", "",
		"Path to a JSON file containing a WMO Core Metadata Profile (Version 2) document.")
	flags.StringP("center", "c", "", "WMO center identifier used to generate the message topic")
	flags.StringP("topic", "t", "origin/a/wis2/{{.Center}}/metadata/core/wcmp2",
		"Topic (template) to use for the message. This is not normally necessary")
	flags.Bool("insecure", false, "If using TLS, don't verify the remote server certificate")

	cobra.CheckErr(cobra.MarkFlagRequired(flags, "broker"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "input"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "center"))

	rootCmd.AddCommand(metaCmd)
}

func doMetaCmd(
	ctx context.Context,
	brokerURL *url.URL,
	input, topic, center string,
	verbose, dryrun, insecure bool,
) {
	if verbose {
		log.Printf("connecting to %+s", brokerURL)
	}

	body, err := os.ReadFile(input)
	if err != nil {
		log.Fatalf("failed to read input file: %s", err)
	}

	if err != nil {
		log.Fatalf("failed to construct message from input: %s", err)
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
