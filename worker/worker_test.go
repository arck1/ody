package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"ody/execution"
	"ody/task"
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

func TestRunWithoutFxStopsWithContext(t *testing.T) {
	definition := task.New[struct{}, struct{}]("standalone.task")
	module, err := task.NewModule("standalone", task.Handle(definition, func(context.Context, task.Message[struct{}]) (struct{}, error) {
		return struct{}{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := task.NewRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := New(execution.NewMemoryStore(), registry, nil, nil, Options{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("standalone worker did not stop")
	}
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

func TestRunStopsAfterInfrastructureFailureLimit(t *testing.T) {
	definition := task.New[struct{}, struct{}]("failing.store")
	module, _ := task.NewModule("failing", task.Handle(definition, func(context.Context, task.Message[struct{}]) (struct{}, error) {
		return struct{}{}, nil
	}))
	registry, _ := task.NewRegistry(module)
	store := &reapFailureStore{MemoryStore: execution.NewMemoryStore()}
	observer := &recordingObserver{}
	runner, err := New(store, registry, nil, observer, Options{
		PollInterval: time.Millisecond, MaxConsecutiveErrors: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = runner.Run(context.Background())
	if err == nil || !errors.Is(err, errInfrastructure) {
		t.Fatalf("Run error = %v, want infrastructure error", err)
	}
	health := runner.Health()
	if health.Status != HealthDegraded || health.ConsecutiveFailures != 2 {
		t.Fatalf("health = %+v", health)
	}
	if len(observer.operations) != 2 || observer.operations[0] != OperationReap {
		t.Fatalf("operations = %v", observer.operations)
	}
}

func TestRunWaitsForInFlightHandlerWithinGracePeriod(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	definition := task.New[struct{}, struct{}]("shutdown.task")
	module, _ := task.NewModule("shutdown", task.Handle(definition, func(context.Context, task.Message[struct{}]) (struct{}, error) {
		close(started)
		<-release
		return struct{}{}, nil
	}))
	registry, _ := task.NewRegistry(module)
	store := execution.NewMemoryStore()
	_, _ = definition.Enqueue(context.Background(), store, struct{}{})
	runner, _ := New(store, registry, nil, nil, Options{
		PollInterval: time.Millisecond, LeaseDuration: time.Second,
		HeartbeatInterval: 100 * time.Millisecond, ShutdownGracePeriod: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-started
	cancel()
	select {
	case err := <-done:
		t.Fatalf("worker stopped before handler: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
}

type lostLeaseStore struct{ *execution.MemoryStore }

func (s *lostLeaseStore) Heartbeat(context.Context, uuid.UUID, uuid.UUID, time.Duration) error {
	return execution.ErrLeaseLost
}

var errInfrastructure = errors.New("infrastructure unavailable")

type reapFailureStore struct{ *execution.MemoryStore }

func (s *reapFailureStore) ReapExpired(context.Context) ([]uuid.UUID, error) {
	return nil, errInfrastructure
}

type recordingObserver struct {
	operations []Operation
}

func (o *recordingObserver) Transition(context.Context, execution.Execution, execution.Status, error) {
}

func (o *recordingObserver) InfrastructureError(_ context.Context, operation Operation, _ error) {
	o.operations = append(o.operations, operation)
}

type assertError string

func (e assertError) Error() string { return string(e) }
