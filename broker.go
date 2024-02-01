package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

const (
	QosAtMostOnce  byte = 0
	QosAtLeastOnce byte = 1
	QosExactlyOnce byte = 2
)

const (
	defaultSSLPort = 8883
	defaultPort    = 1883
)

func getEnvCredentials() (string, string, error) {
	pfx := "WISPUB_BROKER"
	if _, ok := os.LookupEnv(pfx + "_USER"); !ok {
		return "", "", fmt.Errorf(pfx + "_USER not set")
	}
	if _, ok := os.LookupEnv(pfx + "_PASSWD"); !ok {
		return "", "", fmt.Errorf(pfx + "_PASSWD is not set")
	}
	return os.Getenv(pfx + "_USER"), os.Getenv(pfx + "_PASSWD"), nil
}

func newTLSConfig(ca string, insecure bool) (*tls.Config, error) {
	cfg := &tls.Config{}

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		panic(err)
	}
	if ca != "" {
		certs, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("reading CA cert: %w", err)
		}
		if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
			return nil, fmt.Errorf("failed to configure CA cert")
		}
	}
	cfg.InsecureSkipVerify = insecure
	cfg.RootCAs = rootCAs
	return cfg, nil
}

func setDefaultPort(u *url.URL) {
	switch u.Scheme {
	case "ssl":
		if u.Port() == "" {
			u.Host = fmt.Sprintf("%s:%v", u.Host, defaultSSLPort)
		}
	case "tcp":
		if u.Port() == "" {
			u.Host = fmt.Sprintf("%s:%v", u.Host, defaultPort)
		}
	}
}

func newConn(u *url.URL, tlsCA string, insecure bool) (net.Conn, error) {
	switch u.Scheme {
	case "ssl":
		cfg, err := newTLSConfig(tlsCA, insecure)
		if err != nil {
			return nil, err
		}
		return tls.Dial("tcp", u.Host, cfg)
	case "tcp":
		return net.Dial("tcp", u.Host)
	}
	return nil, fmt.Errorf("unsupported url scheme: %s", u.Scheme)
}

// NewClient returns a new connected client
func newClient(ctx context.Context, broker *url.URL, clientID, tlsCA string, insecure bool) (*paho.Client, error) {
	conn, err := newConn(broker, tlsCA, insecure)
	if err != nil {
		return nil, fmt.Errorf("setting up connection: %w", err)
	}
	client := paho.NewClient(paho.ClientConfig{
		ClientID: clientID,
		Conn:     conn,
	})

	user, passwd, err := getEnvCredentials()
	if err != nil {
		return nil, err
	}
	connect := &paho.Connect{
		KeepAlive:    30,
		ClientID:     clientID,
		CleanStart:   true,
		Username:     user,
		UsernameFlag: true,
		Password:     []byte(passwd),
		PasswordFlag: true,
	}

	ack, err := client.Connect(ctx, connect)
	if err != nil {
		return nil, fmt.Errorf("connecting client: %w", err)
	}
	if ack.ReasonCode != 0 {
		return nil, fmt.Errorf("failed to connect [%v] %v", ack.ReasonCode, ack.Properties.ReasonString)
	}

	return client, nil
}

func pubReason(code byte) string {
	switch code {
	case 0:
		return "success"
	case 16:
		return "no subscribers"
	case 128:
		return "unspecified error"
	case 131:
		return "not accepted"
	case 135:
		return "not authorized"
	case 144:
		return "invalid topic name"
	case 151:
		return "quote exceeded"
	case 153:
		return "invalid payload format"
	}
	pa := &packets.Puback{ReasonCode: code}
	s := pa.Reason()
	if s != "" {
		return s
	}
	return "Unknown"
}
