package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"schedulor/execution"
)

func TestTypedTaskPersistsInputAndOutput(t *testing.T) {
	type input struct{ Value int }
	definition := New[input, int]("double", WithVersion(2))
	module, err := NewModule("math", Handle(definition, func(_ context.Context, message Message[input]) (int, error) { return message.Input.Value * 2, nil }))
	if err != nil {
		t.Fatal(err)
	}
	registry, _ := NewRegistry(module)
	store := execution.NewMemoryStore()
	item, err := definition.Enqueue(context.Background(), store, input{Value: 5})
	if err != nil {
		t.Fatal(err)
	}
	claimed, _ := store.Claim(context.Background(), "worker", registry.Names(), 1, time.Minute)
	output, err := registry.Execute(context.Background(), claimed[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != "10" || item.TaskVersion != 2 {
		t.Fatalf("output=%s version=%d", output, item.TaskVersion)
	}
}

func TestDecodeErrorIsPermanent(t *testing.T) {
	definition := New[struct{ Value int }, int]("typed")
	module, _ := NewModule("module", Handle(definition, func(context.Context, Message[struct{ Value int }]) (int, error) { return 0, nil }))
	registry, _ := NewRegistry(module)
	_, err := registry.Execute(context.Background(), execution.Execution{TaskName: "typed", TaskVersion: 1, Input: []byte(`{"Value":"bad"}`)})
	if err == nil || !IsPermanent(err) {
		t.Fatalf("expected permanent decode error, got %v", err)
	}
}

func TestRetryAfterClassification(t *testing.T) {
	want := errors.New("busy")
	err := RetryAfter(want, 3*time.Second)
	delay, ok := RetryDelay(err)
	if !ok || delay != 3*time.Second || !errors.Is(err, want) {
		t.Fatalf("unexpected retry classification")
	}
}
