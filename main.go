package main

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/spf13/cobra"
)

var version = "<notset>"

var (
	broker      string
	input       string
	dataDomain  string
	datetimeVal string
	download    string
	topic       string
	center      string
	satellite   string
	observation string
	mimeType    string
	verbose     bool
	insecure    bool
	dryrun      bool
)

var Cmd = &cobra.Command{
	Use:   "wispub",
	Short: "Publisher for WIS 2.0 messages",
	Long: `Tool for publishing product messages to a WIS 2.0 MQTT broker.

See: https://community.wmo.int/activity-areas/wis/wis2-implementation
Project: https://github.com/bmflynn/wispub
`,
	Example: `
export WISPUB_BROKER_USER=<username>
export WISPUB_BROKER_PASSWD=<password>

wispub \
	--broker=ssl://<broker host> \
	--topic=<message topic> \
	--download-url=<product download url> \
	--input=<product file>

Add the --dryrun flag to print out message information without sending.

More information on topic hierarchy is available here: 
	https://github.com/wmo-im/wis2-topic-hierarchy
`,
	Args:    cobra.NoArgs,
	Version: version,
	RunE: func(cmd *cobra.Command, args []string) error {
		brokerURL, err := url.Parse(broker)
		if err != nil {
			return fmt.Errorf("invalid download URL")
		}

		downloadURL, err := url.Parse(download)
		if err != nil {
			return fmt.Errorf("invalid download URL")
		}

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

		setDefaultPort(brokerURL)

		ctx := exitHandlerContext()

		run(ctx, brokerURL, downloadURL)
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.BoolVar(&verbose, "verbose", false, "Verbose logging")
	flags.BoolVar(&dryrun, "dryrun", false, "Generate and print the message and topic, but don't send")

	flags.StringVar(&broker, "broker", "",
		"MQTT broker URL to publish messages to. Can be tcp:// or ssl://. If the port is not included it "+
			"will default to "+fmt.Sprintf("%v for tcp and %v for ssl.", defaultPort, defaultSSLPort))
	flags.StringVarP(&input, "input", "i", "", "Path to the file to send")
	flags.StringVarP(&download, "download-url", "u", "", "Publicly available URL where the data can be downloaded")
	flags.StringVarP(&topic, "topic", "t", "", "Topic to publish the message to")
	flags.BoolVar(&insecure, "insecure", false, "If using TLS, don't verify the remote server certificate")
	flags.StringVarP(&mimeType, "mime-type", "m", "", "Mime-type for the provided input. If not provided it will be determined by file extension.")
	flags.StringVarP(&dataDomain, "data-domain", "d", "DBNet", "Data domain indicator to add to the message properties.dataDomain")
	flags.StringVarP(&datetimeVal, "datetime", "D", "",
		"Time and date of the data as either a single timestamp or as a comma separated start and end. The format for "+
			"the timestamp(s) is RFC3339, e.g., <yyyy-mm-dd>T<hh:mm:ss>Z")

	cobra.CheckErr(cobra.MarkFlagRequired(flags, "broker"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "download-url"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "topic"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "input"))
	cobra.CheckErr(cobra.MarkFlagRequired(flags, "datetime"))
}

func main() {
	if err := Cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func exitHandlerContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		log.Printf("shutdown with signal '%v'", <-ch)
		cancel()
	}()
	return ctx
}

func parseDatetime() (string, string, error) {
	layout := "2006-01-02T15:04:05Z"
	start, end, found := strings.Cut(datetimeVal, ",")
	if found {
		startT, err := time.Parse(time.RFC3339, start)
		if err != nil {
			return "", "", fmt.Errorf("invalid datetime start value: %s", start)
		}
		endT, err := time.Parse(time.RFC3339, end)
		if err != nil {
			return "", "", fmt.Errorf("invalid datetime end value: %s", end)
		}
		return startT.Format(layout), endT.Format(layout), nil
	}
	t, err := time.Parse(time.RFC3339, datetimeVal)
	if err != nil {
		return "", "", fmt.Errorf("invalid datetime value: %s", datetimeVal)
	}
	// Mon Jan 2 15:04:05 MST 2006
	return t.Format("2006-01-02T15:04:05Z"), "", nil
}

func run(ctx context.Context, brokerURL, downloadURL *url.URL) {
	if verbose {
		log.Printf("connecting to %+s", brokerURL)
	}

	start, end, err := parseDatetime()
	if err != nil {
		log.Fatalf("failed to parse timestamps: %s", err)
	}

	wisMsg, err := newMessage(input, topic, downloadURL, mimeType, start, end)
	if err != nil {
		log.Fatalf("failed to construct message from input: %s", err)
	}

	var properties map[string]any
	if dataDomain != "" {
		properties = map[string]any{"dataDomain": dataDomain}
	}
	body, err := encodeWithAdditionalProperties(wisMsg, properties)
	if err != nil {
		log.Fatalf("failed to encode message as json: %s", err)
	}

	if dryrun {
		os.Stderr.WriteString(topic + "\n")
		os.Stdout.Write(body)
		os.Stdout.WriteString("\n")
		return
	}

	client, err := newClient(ctx, brokerURL, strings.ToLower(center), "", insecure)
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
		log.Printf("unexpected publish response [code=%v]: %v", zult.ReasonCode, pubReason(zult.ReasonCode))
	}
}
