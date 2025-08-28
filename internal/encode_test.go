package internal

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_encodeWithAdditionalProperties(t *testing.T) {
	msg := &NotificationMsgV04{
		ID:         "ID",
		ConformsTo: []string{},
		Type:       "TYPE",
		Geometry:   nil,
		Properties: NotificationMsgV04Properties{
			DataID:  "DATAID",
			PubTime: "PUBTIME",
			Integrity: Integrity{
				Method: "METHOD",
				Value:  "VALUE",
			},
		},
		Links: []Link{
			{Href: "http://...", Rel: "canonical", Length: 999, Type: "text/plain"},
		},
	}

	dat, err := EncodeMessage(msg, map[string]any{
		"myNewProperty":     true,
		"myOtherProperty":   0,
		"myAnotherProperty": "XXX",
	})
	require.NoError(t, err)

	replace := strings.NewReplacer(" ", "", "\n", "")
	expected := replace.Replace(`
{
    "conformsTo": [],
    "geometry": null,
    "id": "ID",
    "links": [{
      "href": "http://...",
      "rel": "canonical",
      "type": "text/plain",
      "length": 999
    }],
    "properties": {
      "data_id": "DATAID",
      "integrity": {
        "method": "METHOD",
        "value": "VALUE"
      },
      "myAnotherProperty": "XXX",
      "myNewProperty": true,
      "myOtherProperty": 0,
      "pubtime": "PUBTIME"
    },
    "type": "TYPE"
}`)
	require.Equal(t, expected, replace.Replace(string(dat)))
}

func Test_encodeDatetimes(t *testing.T) {
	msg := &NotificationMsgV04{
		ID:         "ID",
		ConformsTo: []string{},
		Type:       "TYPE",
		Geometry:   nil,
		Properties: NotificationMsgV04Properties{
			DataID:  "DATAID",
			PubTime: "PUBTIME",
			Integrity: Integrity{
				Method: "METHOD",
				Value:  "VALUE",
			},
		},
		Links: []Link{
			{Href: "http://...", Rel: "canonical", Length: 999, Type: "text/plain"},
		},
	}

	t.Run("with", func(t *testing.T) {
		msg.Properties.Datetime = "xxx"
		dat, err := EncodeMessage(msg, nil)
		require.NoError(t, err)

		require.Contains(t, string(dat), "datetime")
	})
	t.Run("without", func(t *testing.T) {
		msg.Properties.Datetime = ""
		dat, err := EncodeMessage(msg, nil)
		require.NoError(t, err)

		require.NotContains(t, string(dat), "datetime")
	})
}
