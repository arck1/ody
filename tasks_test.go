package schedulor

import (
	"context"
	"errors"
	"strings"
	"testing"

	queue2 "schedulor/queue"
)

func TestCodeTaskExecutorExecute(t *testing.T) {
	exec := NewCodeTaskExecutor([]TaskHandler{
		{
			TaskName: "ok",
			Handler: func(ctx context.Context, payload map[string]interface{}) error {
				if payload["foo"] != "bar" {
					t.Fatalf("unexpected payload: %+v", payload)
				}
				return nil
			},
		},
	})

	payload, err := queue2.NewJSONPayload(map[string]any{"foo": "bar"})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}

	if err = exec.Execute(context.Background(), queue2.Claimed{TaskID: 1, TaskName: "ok", Payload: payload}); err != nil {
		t.Fatalf("execute error: %v", err)
	}
}

func TestCodeTaskExecutorUnknownTask(t *testing.T) {
	exec := NewCodeTaskExecutor(nil)
	payload, _ := queue2.NewJSONPayload(map[string]any{"foo": "bar"})
	err := exec.Execute(context.Background(), queue2.Claimed{TaskID: 77, TaskName: "missing", Payload: payload})
	var unknown *UnknownTaskName
	if !errors.As(err, &unknown) {
		t.Fatalf("expected UnknownTaskName, got: %v", err)
	}
}

func TestBashTaskExecutorExecute(t *testing.T) {
	exec := NewBashTaskExecutor([]string{"bash"}, "command")
	payload, err := queue2.NewJSONPayload(map[string]any{"command": "echo ok"})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}

	if err = exec.Execute(context.Background(), queue2.Claimed{TaskID: 1, TaskName: "bash", Payload: payload}); err != nil {
		t.Fatalf("execute error: %v", err)
	}
}

func TestBashTaskExecutorValidation(t *testing.T) {
	exec := NewBashTaskExecutor(nil, "")

	payloadMissing, _ := queue2.NewJSONPayload(map[string]any{"x": "echo ok"})
	err := exec.Execute(context.Background(), queue2.Claimed{TaskID: 1, TaskName: "bash", Payload: payloadMissing})
	if err == nil || !strings.Contains(err.Error(), "payload field \"command\" is required") {
		t.Fatalf("unexpected error for missing command: %v", err)
	}

	payloadBadType, _ := queue2.NewJSONPayload(map[string]any{"command": 42})
	err = exec.Execute(context.Background(), queue2.Claimed{TaskID: 1, TaskName: "bash", Payload: payloadBadType})
	if err == nil || !strings.Contains(err.Error(), "must be non-empty string") {
		t.Fatalf("unexpected error for bad command type: %v", err)
	}
}

func TestExecutorProcessTaskPanic(t *testing.T) {
	panicExec := &panicTaskExecutor{}
	e := NewLqExecutorWith(nil, testLogger(), nil, panicExec, &LqExecutorOptions{PoolingTimeout: 1, PoolingBatch: 1})

	payload, _ := queue2.NewJSONPayload(map[string]any{"foo": "bar"})
	err := e.processTask(context.Background(), queue2.Claimed{TaskID: 99, TaskName: "panic", Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "task handler panic") {
		t.Fatalf("expected panic error, got: %v", err)
	}
}

type panicTaskExecutor struct{}

func (p *panicTaskExecutor) TaskNames() []string { return []string{"panic"} }

func (p *panicTaskExecutor) Execute(ctx context.Context, task queue2.Claimed) error {
	panic("boom")
}
