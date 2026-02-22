package queue

import "encoding/json"

type JSONPayload []byte

// NewJSONPayload marshals map payload into queue JSON bytes.
func NewJSONPayload(data map[string]any) (JSONPayload, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return JSONPayload(raw), nil
}

// Data unmarshals payload bytes into map; invalid payload returns empty map.
func (p JSONPayload) Data() map[string]any {
	if len(p) == 0 {
		return map[string]any{}
	}
	var data map[string]any
	if err := json.Unmarshal(p, &data); err != nil {
		return map[string]any{}
	}
	if data == nil {
		return map[string]any{}
	}
	return data
}
