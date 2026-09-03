package executor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"schedulor/queue"
)

func TestUnknownTaskNameError(t *testing.T) {
	err := (&UnknownTaskName{TaskId: 42, TaskName: "x"}).Error()
	if !strings.Contains(err, "x") || !strings.Contains(err, "42") {
		t.Fatalf("unexpected error string: %s", err)
	}
}

func TestCodeTaskExecutor(t *testing.T) {
	exec := NewCodeTaskExecutor([]TaskHandler{{
		TaskName: "ok",
		Handler: func(ctx context.Context, payload map[string]any) error {
			if payload["k"] != "v" {
				t.Fatalf("unexpected payload: %+v", payload)
			}
			return nil
		},
	}})

	if len(exec.TaskNames()) != 1 {
		t.Fatalf("expected 1 task name, got %d", len(exec.TaskNames()))
	}

	if err := exec.Execute(context.Background(), claimedWithPayload(t, "ok", map[string]any{"k": "v"})); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	err := exec.Execute(context.Background(), claimedWithPayload(t, "missing", map[string]any{}))
	var unknown *UnknownTaskName
	if !errors.As(err, &unknown) {
		t.Fatalf("expected UnknownTaskName, got: %v", err)
	}
}

func TestBashTaskExecutorFromPayload(t *testing.T) {
	exec := NewBashTaskExecutor(nil, "")
	if len(exec.TaskNames()) != 1 || exec.TaskNames()[0] != "bash" {
		t.Fatalf("unexpected default task names: %+v", exec.TaskNames())
	}

	if err := exec.Execute(context.Background(), claimedWithPayload(t, "bash", map[string]any{"command": "echo ok"})); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	err := exec.Execute(context.Background(), claimedWithPayload(t, "bash", map[string]any{"x": "echo"}))
	if err == nil || !strings.Contains(err.Error(), "is required") {
		t.Fatalf("unexpected missing command error: %v", err)
	}

	err = exec.Execute(context.Background(), claimedWithPayload(t, "bash", map[string]any{"command": 123}))
	if err == nil || !strings.Contains(err.Error(), "must be string or array of strings") {
		t.Fatalf("unexpected bad command type error: %v", err)
	}

	if err := exec.Execute(
		context.Background(),
		claimedWithPayload(t, "bash", map[string]any{"command": []any{"echo", "ok"}}),
	); err != nil {
		t.Fatalf("unexpected array command error: %v", err)
	}
}

func TestBashTaskExecutorFromCommands(t *testing.T) {
	exec := NewBashTaskExecutorFromCommands(map[string]string{
		"a": "echo a",
		"b": "echo b",
	})
	if len(exec.TaskNames()) != 2 {
		t.Fatalf("expected 2 task names, got %d", len(exec.TaskNames()))
	}

	if err := exec.Execute(context.Background(), claimedWithPayload(t, "a", map[string]any{})); err != nil {
		t.Fatalf("execute error: %v", err)
	}

	err := exec.Execute(context.Background(), claimedWithPayload(t, "missing", map[string]any{}))
	var unknown *UnknownTaskName
	if !errors.As(err, &unknown) {
		t.Fatalf("expected UnknownTaskName, got: %v", err)
	}
}

func TestBashTaskExecutorWithCommandsArgs(t *testing.T) {
	exec := NewBashTaskExecutorWithCommands(map[string]TaskCommand{
		"health": {Args: []string{"echo", "ok"}},
	})
	if err := exec.Execute(context.Background(), claimedWithPayload(t, "health", map[string]any{})); err != nil {
		t.Fatalf("execute error: %v", err)
	}
}

func claimedWithPayload(t *testing.T, taskName string, payload map[string]any) queue.Claimed {
	t.Helper()
	p, err := queue.NewJSONPayload(payload)
	if err != nil {
		t.Fatalf("payload marshal error: %v", err)
	}
	return queue.Claimed{TaskID: 1, TaskName: taskName, Payload: p}
}
