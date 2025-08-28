package internal

import (
	"crypto/sha512"
	_ "embed"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

func init() {
	for typ, exts := range map[string][]string{
		"application/bufr": {".bufr", ".bufr.bin"},
		"application/grib": {".grib", ".grib.bin"},
	} {
		for _, ext := range exts {
			if err := mime.AddExtensionType(ext, typ); err != nil {
				panic(err)
			}
		}
	}
}

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

// Get mime type from file name
// See additional types registered in init
func mimeTypeByExtension(name string) string {
	switch {
	case strings.HasSuffix(name, ".bufr.bin"):
		name = strings.Replace(name, ".bufr.bin", ".bufr", 1)
	case strings.HasSuffix(name, ".grib.bin"):
		name = strings.Replace(name, ".grib.bin", ".grib", 1)
	}
	ext := path.Ext(name)
	if ext == "" {
		// no extension
		return "application/octet-stream"
	}
	typ := mime.TypeByExtension(ext)
	if typ == "" {
		// unknown mimetype
		return "application/octet-stream"
	}
	return typ
}

func NewNotificationMessage(fpath, topic string, downloadURL *url.URL, mimeType, metaId, start, end string) (*NotificationMsgV04, error) {
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

	typ := mimeType
	if typ == "" {
		typ = mimeTypeByExtension(fpath)
	}

	props := NotificationMsgV04Properties{
		DataID:    dataID,
		MetaId:    metaId,
		PubTime:   time.Now().Format("2006-01-02T15:04:05.000000000Z"),
		Integrity: *csum,
	}

	if start != "" && end == "" {
		props.Datetime = start
	} else if start != "" && end != "" {
		props.StartDatetime = start
		props.EndDatetime = end
	}

	return &NotificationMsgV04{
		ID:         genMessageID(),
		ConformsTo: []string{"http://wis.wmo.int/spec/wnm/1/conf/core"},
		Type:       "Feature",
		Geometry:   nil,
		Properties: props,
		Links: []Link{
			{Href: downloadURL.String(), Rel: "canonical", Type: typ, Length: fi.Size()},
		},
	}, nil
}

type Integrity struct {
	Method string `json:"method"`
	Value  string `json:"value"`
}

type Link struct {
	Href   string `json:"href"`
	Rel    string `json:"rel"`
	Type   string `json:"type"`
	Length int64  `json:"length"`
}

type NotificationMsgV04Properties struct {
	DataID        string    `json:"data_id"`
	PubTime       string    `json:"pubtime"`
	Integrity     Integrity `json:"integrity"`
	MetaId        string    `json:"metadata_id,omitempty"`
	Datetime      string    `json:"datetime,omitempty"`
	StartDatetime string    `json:"start_datetime,omitempty"`
	EndDatetime   string    `json:"end_datetime,omitempty"`
}

type NotificationMsgV04 struct {
	ID         string                       `json:"id"`
	ConformsTo []string                     `json:"conformsTo"`
	Type       string                       `json:"type"`
	Geometry   any                          `json:"geometry"`
	Properties NotificationMsgV04Properties `json:"properties"`
	Links      []Link                       `json:"links"`
}
