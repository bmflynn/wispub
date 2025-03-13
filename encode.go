package main

import (
	"encoding/json"
	"fmt"
)

func encode(msg *MsgV04) ([]byte, error) {
	return json.MarshalIndent(msg, "", "  ")
}

func encodeWithAdditionalProperties(msg *MsgV04, properties map[string]any) ([]byte, error) {
	dat, err := encode(msg)
	if err != nil {
		return nil, err
	}
	if properties == nil {
		return dat, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(dat, &raw); err != nil {
		return nil, fmt.Errorf("redecoding: %w", err)
	}

	var existing map[string]any
	if err := json.Unmarshal(raw["properties"], &existing); err != nil {
		return nil, fmt.Errorf("redecoding properties: %w", err)
	}

	for k, v := range properties {
		existing[k] = v
	}
	raw["properties"], err = json.MarshalIndent(existing, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("reencoding properties: %w", err)
	}

	return json.MarshalIndent(raw, "", "  ")
}
