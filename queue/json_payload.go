package queue

import "encoding/json"

type JSONPayload []byte

func NewJSONPayload(data map[string]any) (JSONPayload, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return JSONPayload(raw), nil
}

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
