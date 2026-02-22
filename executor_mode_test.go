package schedulor

import (
	"context"
	"testing"
)

func TestNewLqExecutorUsesProvidedInterfaces(t *testing.T) {
	strategy := NewCodeTaskExecutor([]TaskHandler{{
		TaskName: "ok",
		Handler: func(ctx context.Context, payload map[string]interface{}) error {
			return nil
		},
	}})
	backend := &testQueueBackend{}
	exec := NewLqExecutor(testLogger(), backend, strategy, &LqExecutorOptions{PoolingTimeout: 10, PoolingBatch: 1})

	if exec.queue != backend {
		t.Fatalf("expected provided backend to be used")
	}
	if exec.exec != strategy {
		t.Fatalf("expected provided strategy to be used")
	}
}

func TestNewLqExecutorPanicsOnNilBackend(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic for nil backend")
		}
	}()
	_ = NewLqExecutor(testLogger(), nil, NewCodeTaskExecutor(nil), &LqExecutorOptions{PoolingTimeout: 10, PoolingBatch: 1})
}

func TestNewLqExecutorPanicsOnNilExecutor(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic for nil executor")
		}
	}()
	_ = NewLqExecutor(testLogger(), &testQueueBackend{}, nil, &LqExecutorOptions{PoolingTimeout: 10, PoolingBatch: 1})
}
