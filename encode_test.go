package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_encodeWithAdditionalProperties(t *testing.T) {
	msg := &MsgV04{
		ID:         "ID",
		ConformsTo: []string{},
		Type:       "TYPE",
		Geometry:   nil,
		Properties: MsgV04Properties{
			DataID:  "DATAID",
			PubTime: "PUBTIME",
			Integrity: Integrity{
				Method: "METHOD",
				Value:  "VALUE",
			},
			Size: 999,
		},
		Links: []Link{},
	}

	dat, err := encodeWithAdditionalProperties(msg, map[string]any{
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
    "links": [],
    "properties": {
      "data_id": "DATAID",
      "integrity": {
        "method": "METHOD",
        "value": "VALUE"
      },
      "myAnotherProperty": "XXX",
      "myNewProperty": true,
      "myOtherProperty": 0,
      "pubtime": "PUBTIME",
      "size": 999
    },
    "type": "TYPE"
}`)
	require.Equal(t, expected, replace.Replace(string(dat)))
}
