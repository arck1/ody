package schedulor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"schedulor/queue"
)

func TestCodeTaskExecutorExecute(t *testing.T) {
	exec := NewCodeTaskExecutor([]TaskHandler{
		{
			TaskName: "ok",
			Handler: func(ctx context.Context, payload map[string]any) error {
				if payload["foo"] != "bar" {
					t.Fatalf("unexpected payload: %+v", payload)
				}
				return nil
			},
		},
	})

	payload, err := queue.NewJSONPayload(map[string]any{"foo": "bar"})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}

	if err = exec.Execute(context.Background(), queue.Claimed{TaskID: 1, TaskName: "ok", Payload: payload}); err != nil {
		t.Fatalf("execute error: %v", err)
	}
}

func TestCodeTaskExecutorUnknownTask(t *testing.T) {
	exec := NewCodeTaskExecutor(nil)
	payload, _ := queue.NewJSONPayload(map[string]any{"foo": "bar"})
	err := exec.Execute(context.Background(), queue.Claimed{TaskID: 77, TaskName: "missing", Payload: payload})
	if _, ok := errors.AsType[*UnknownTaskName](err); !ok {
		t.Fatalf("expected UnknownTaskName, got: %v", err)
	}
}

func TestBashTaskExecutorExecute(t *testing.T) {
	exec := NewBashTaskExecutor([]string{"bash"}, "command")
	payload, err := queue.NewJSONPayload(map[string]any{"command": "echo ok"})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}

	if err = exec.Execute(context.Background(), queue.Claimed{TaskID: 1, TaskName: "bash", Payload: payload}); err != nil {
		t.Fatalf("execute error: %v", err)
	}
}

func TestBashTaskExecutorValidation(t *testing.T) {
	exec := NewBashTaskExecutor(nil, "")

	payloadMissing, _ := queue.NewJSONPayload(map[string]any{"x": "echo ok"})
	err := exec.Execute(context.Background(), queue.Claimed{TaskID: 1, TaskName: "bash", Payload: payloadMissing})
	if err == nil || !strings.Contains(err.Error(), "payload field \"command\" is required") {
		t.Fatalf("unexpected error for missing command: %v", err)
	}

	payloadBadType, _ := queue.NewJSONPayload(map[string]any{"command": 42})
	err = exec.Execute(context.Background(), queue.Claimed{TaskID: 1, TaskName: "bash", Payload: payloadBadType})
	if err == nil || !strings.Contains(err.Error(), "must be string or array of strings") {
		t.Fatalf("unexpected error for bad command type: %v", err)
	}
}

func TestExecutorProcessTaskPanic(t *testing.T) {
	panicExec := &panicTaskExecutor{}
	e, err := NewLqExecutor(testLogger(), &testQueueBackend{}, panicExec, &LqExecutorOptions{PoolingTimeout: 1, PoolingBatch: 1})
	if err != nil {
		t.Fatalf("NewLqExecutor error: %v", err)
	}

	payload, _ := queue.NewJSONPayload(map[string]any{"foo": "bar"})
	err = e.processTask(context.Background(), queue.Claimed{TaskID: 99, TaskName: "panic", Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "task handler panic") {
		t.Fatalf("expected panic error, got: %v", err)
	}
}

func TestBashTaskExecutorFromCommands(t *testing.T) {
	exec := NewBashTaskExecutorFromCommands(map[string]string{
		"task_a": "echo a",
		"task_b": "echo b",
	})
	names := exec.TaskNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 task names, got %d", len(names))
	}

	if err := exec.Execute(context.Background(), queue.Claimed{TaskID: 1, TaskName: "task_a"}); err != nil {
		t.Fatalf("execute task_a: %v", err)
	}
}

func TestBashTaskExecutorWithCommands(t *testing.T) {
	exec := NewBashTaskExecutorWithCommands(map[string]BashTaskCommand{
		"task_a": {Args: []string{"echo", "ok"}},
	})
	if err := exec.Execute(context.Background(), queue.Claimed{TaskID: 1, TaskName: "task_a"}); err != nil {
		t.Fatalf("execute task_a: %v", err)
	}
}

type panicTaskExecutor struct{}

func (p *panicTaskExecutor) TaskNames() []string { return []string{"panic"} }

func (p *panicTaskExecutor) Execute(ctx context.Context, task queue.Claimed) error {
	panic("boom")
}
