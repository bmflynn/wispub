package main

import "encoding/json"

func encode(msg *MsgV04) ([]byte, error) {
	return json.MarshalIndent(msg, "", "  ")
}
