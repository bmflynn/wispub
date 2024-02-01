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

	"github.com/eclipse/paho.golang/paho"
	"github.com/spf13/cobra"
)

var version = "<notset>"

var (
	broker      string
	input       string
	download    string
	topic       string
	center      string
	satellite   string
	observation string
	verbose     bool
	insecure    bool
	dryrun      bool
)

var Cmd = &cobra.Command{
	Use:   "wispub",
	Short: "Publisher for WIS 2.0 messages",
	Long: `Tool for publishing product messages to a WIS 2.0 MQTT broker.

See: https://community.wmo.int/activity-areas/wis/wis2-implementation
Project: https://gitlab.ssec.wisc.edu/dbrtn/wispub
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

	cobra.MarkFlagRequired(flags, "broker")
	cobra.MarkFlagRequired(flags, "download-url")
	cobra.MarkFlagRequired(flags, "topic")
	cobra.MarkFlagRequired(flags, "input")
}

func main() {
	Cmd.Execute()
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

func run(ctx context.Context, brokerURL, downloadURL *url.URL) {
	if verbose {
		log.Printf("connecting to %+s", brokerURL)
	}

	wisMsg, err := newMessage(input, topic, downloadURL)
	if err != nil {
		log.Fatalf("failed to construct message from input: %s", err)
	}

	body, err := encode(wisMsg)
	if err != nil {
		log.Fatalf("failed to encode message as json: %s", err)
	}

	if dryrun {
		os.Stdout.WriteString(topic + "\n")
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
