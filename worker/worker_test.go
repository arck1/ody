package worker

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"schedulor/execution"
	"schedulor/task"
)

func TestLeaseLossCancelsHandlerContext(t *testing.T) {
	definition := task.New[struct{}, struct{}]("lease.task")
	cancelled := make(chan struct{})
	module, _ := task.NewModule("lease", task.Handle(definition, func(ctx context.Context, _ task.Message[struct{}]) (struct{}, error) {
		<-ctx.Done()
		close(cancelled)
		return struct{}{}, ctx.Err()
	}))
	registry, _ := task.NewRegistry(module)
	store := &lostLeaseStore{MemoryStore: execution.NewMemoryStore()}
	_, _ = definition.Enqueue(context.Background(), store, struct{}{})
	runner, err := New(store, registry, nil, nil, Options{PollInterval: time.Millisecond, LeaseDuration: 50 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = runner.Run(ctx); close(done) }()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("handler context was not cancelled after lease loss")
	}
	cancel()
	<-done
}

func TestPermanentFailureIsStoredWithoutRetry(t *testing.T) {
	definition := task.New[int, int]("permanent.task", task.WithMaxAttempts(5))
	module, _ := task.NewModule("permanent", task.Handle(definition, func(context.Context, task.Message[int]) (int, error) {
		return 0, task.Permanent(assertError("invalid"))
	}))
	registry, _ := task.NewRegistry(module)
	store := execution.NewMemoryStore()
	created, _ := definition.Enqueue(context.Background(), store, 1)
	runner, _ := New(store, registry, nil, nil, Options{PollInterval: time.Millisecond, LeaseDuration: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = runner.Run(ctx); close(done) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		item, _ := store.GetExecution(context.Background(), created.ID)
		if item.Status == execution.StatusFailed {
			if item.Attempt != 1 {
				t.Fatalf("attempt=%d", item.Attempt)
			}
			cancel()
			<-done
			return
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	t.Fatal("permanent failure was not stored")
}

type lostLeaseStore struct{ *execution.MemoryStore }

func (s *lostLeaseStore) Heartbeat(context.Context, uuid.UUID, uuid.UUID, time.Duration) error {
	return execution.ErrLeaseLost
}

type assertError string

func (e assertError) Error() string { return string(e) }
