package schedulor

import (
	"context"
	"testing"
)

func TestNewLqExecutorUsesProvidedInterfaces(t *testing.T) {
	strategy := NewCodeTaskExecutor([]TaskHandler{{
		TaskName: "ok",
		Handler: func(ctx context.Context, payload map[string]any) error {
			return nil
		},
	}})
	backend := &testQueueBackend{}
	exec, err := NewLqExecutor(testLogger(), backend, strategy, &LqExecutorOptions{PoolingTimeout: 10, PoolingBatch: 1})
	if err != nil {
		t.Fatalf("NewLqExecutor error: %v", err)
	}

	if exec.queue != backend {
		t.Fatalf("expected provided backend to be used")
	}
	if exec.exec != strategy {
		t.Fatalf("expected provided strategy to be used")
	}
}

func TestNewLqExecutorReturnsErrorOnNilBackend(t *testing.T) {
	_, err := NewLqExecutor(testLogger(), nil, NewCodeTaskExecutor(nil), &LqExecutorOptions{PoolingTimeout: 10, PoolingBatch: 1})
	if err == nil {
		t.Fatalf("expected error for nil backend")
	}
}

func TestNewLqExecutorReturnsErrorOnNilExecutor(t *testing.T) {
	_, err := NewLqExecutor(testLogger(), &testQueueBackend{}, nil, &LqExecutorOptions{PoolingTimeout: 10, PoolingBatch: 1})
	if err == nil {
		t.Fatalf("expected error for nil executor")
	}
}
