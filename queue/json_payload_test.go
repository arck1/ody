package queue

import "testing"

func TestJSONPayloadData(t *testing.T) {
	payload, err := NewJSONPayload(map[string]any{"foo": "bar", "n": float64(1)})
	if err != nil {
		t.Fatalf("NewJSONPayload error: %v", err)
	}
	data := payload.Data()
	if data["foo"] != "bar" {
		t.Fatalf("unexpected foo: %v", data["foo"])
	}
	if data["n"] != float64(1) {
		t.Fatalf("unexpected n: %v", data["n"])
	}
}

func TestJSONPayloadDataInvalid(t *testing.T) {
	payload := JSONPayload([]byte("{"))
	if data := payload.Data(); len(data) != 0 {
		t.Fatalf("expected empty map for invalid payload, got: %+v", data)
	}
}
