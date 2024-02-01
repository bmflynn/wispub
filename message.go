package main

import (
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

func checksum(f *os.File) (*Integrity, error) {
	h := sha512.New()
	_, err := io.Copy(h, f)
	if err != nil {
		return nil, err
	}
	return &Integrity{
		Method: "sha512",
		Value:  base64.StdEncoding.EncodeToString(h.Sum(nil)),
	}, nil
}

func genMessageID() string { return uuid.New().String() }
func getDataID(topic, filename string) (string, error) {
	parts := strings.Split(topic, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("not enough components")
	}
	parts = parts[2:]
	parts = append(parts, filename)
	return path.Join(parts...), nil
}

func newMessage(fpath, topic string, downloadURL *url.URL) (*MsgV04, error) {
	f, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}

	csum, err := checksum(f)
	if err != nil {
		return nil, fmt.Errorf("checksumming: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("statting: %w", err)
	}

	dataID, err := getDataID(topic, fi.Name())
	if err != nil {
		return nil, fmt.Errorf("unable to construct data id: %w", err)
	}

	return &MsgV04{
		ID:       genMessageID(),
		Version:  "v04",
		Type:     "Feature",
		Geometry: nil,
		Properties: MsgV04Properties{
			DataID:    dataID,
			PubTime:   time.Now().Format("20060102T150405.000000000Z"),
			Integrity: *csum,
			Size:      fi.Size(),
		},
		Links: []Link{
			{Href: downloadURL.String(), Rel: "canonical", Type: "application/octet-stream"},
		},
	}, nil
}

type Integrity struct {
	Method string `json:"method"`
	Value  string `json:"value"`
}

type Link struct {
	Href string `json:"href"`
	Rel  string `json:"rel"`
	Type string `json:"type"`
}

type MsgV04Properties struct {
	DataID    string    `json:"data_id"`
	PubTime   string    `json:"pubtime"`
	Integrity Integrity `json:"integrity"`
	Size      int64     `json:"size"`
}

type MsgV04 struct {
	ID         string           `json:"id"`
	Version    string           `json:"version"`
	Type       string           `json:"type"`
	Geometry   interface{}      `json:"geometry"`
	Properties MsgV04Properties `json:"properties"`
	Links      []Link           `json:"links"`
}
