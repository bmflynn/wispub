package cmd

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"
)

const (
	defaultSSLPort = 8883
	defaultPort    = 1883
)

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

func parseDatetime(value string) (string, string, error) {
	// Not an error to not provided datetime
	if value == "" {
		return "", "", nil
	}
	layout := "2006-01-02T15:04:05Z"
	start, end, found := strings.Cut(value, ",")
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
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return "", "", fmt.Errorf("invalid datetime value: %s", value)
	}
	// Mon Jan 2 15:04:05 MST 2006
	return t.Format("2006-01-02T15:04:05Z"), "", nil
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
