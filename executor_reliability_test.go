package schedulor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"schedulor/queue"
)

func TestExecutorStartsHeartbeatForEntireClaimedBatch(t *testing.T) {
	backend := &reliabilityQueue{
		claimed: []queue.Claimed{
			{TaskID: 1, TaskName: "work", LeaseToken: uuid.New(), Attempts: 1, MaxAttempts: 3},
			{TaskID: 2, TaskName: "work", LeaseToken: uuid.New(), Attempts: 1, MaxAttempts: 3},
		},
		heartbeatStarted: make(chan int64, 2),
	}
	releaseFirst := make(chan struct{})
	exec, err := NewLqExecutor(
		testLogger(),
		backend,
		&reliabilityExecutor{
			names: []string{"work"},
			execute: func(ctx context.Context, task queue.Claimed) error {
				if task.TaskID == 1 {
					select {
					case <-releaseFirst:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
		},
		&LqExecutorOptions{PoolingTimeout: time.Hour, PoolingBatch: 2},
	)
	if err != nil {
		t.Fatalf("NewLqExecutor error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		exec.Run(ctx)
		close(done)
	}()

	started := map[int64]bool{}
	for len(started) < 2 {
		select {
		case taskID := <-backend.heartbeatStarted:
			started[taskID] = true
		case <-time.After(time.Second):
			t.Fatal("heartbeats were not started for the entire batch")
		}
	}
	close(releaseFirst)
	eventually(t, time.Second, func() bool { return backend.acks.Load() == 2 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("executor did not stop")
	}
}

func TestExecutorMovesFinalFailureDirectlyToDLQ(t *testing.T) {
	backend := &reliabilityQueue{
		claimed: []queue.Claimed{{
			TaskID: 7, TaskName: "fail", LeaseToken: uuid.New(), Attempts: 3, MaxAttempts: 3,
		}},
		heartbeatStarted: make(chan int64, 1),
		dlq:              make(chan dlqCall, 1),
	}
	exec, err := NewLqExecutor(
		testLogger(),
		backend,
		&reliabilityExecutor{
			names: []string{"fail"},
			execute: func(context.Context, queue.Claimed) error {
				return errors.New("permanent failure")
			},
		},
		&LqExecutorOptions{PoolingTimeout: time.Hour, PoolingBatch: 1},
	)
	if err != nil {
		t.Fatalf("NewLqExecutor error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		exec.Run(ctx)
		close(done)
	}()
	select {
	case call := <-backend.dlq:
		if call.taskID != 7 || call.errText != "permanent failure" {
			t.Fatalf("unexpected DLQ call: %+v", call)
		}
	case <-time.After(time.Second):
		t.Fatal("task was not moved to DLQ")
	}
	if backend.nacks.Load() != 0 {
		t.Fatalf("final failure must not be nacked before DLQ move")
	}
	cancel()
	<-done
}

func TestExecutorPollingWaitIsCancelable(t *testing.T) {
	exec, err := NewLqExecutor(
		testLogger(),
		&reliabilityQueue{},
		&reliabilityExecutor{names: []string{"work"}},
		&LqExecutorOptions{PoolingTimeout: time.Hour, PoolingBatch: 1},
	)
	if err != nil {
		t.Fatalf("NewLqExecutor error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		exec.Run(ctx)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("executor remained blocked in polling wait")
	}
}

type reliabilityExecutor struct {
	names   []string
	execute func(context.Context, queue.Claimed) error
}

func (e *reliabilityExecutor) TaskNames() []string { return e.names }

func (e *reliabilityExecutor) Execute(ctx context.Context, task queue.Claimed) error {
	if e.execute == nil {
		return nil
	}
	return e.execute(ctx, task)
}

type dlqCall struct {
	taskID  int64
	errText string
	lease   uuid.UUID
}

type reliabilityQueue struct {
	mu               sync.Mutex
	claimed          []queue.Claimed
	heartbeatStarted chan int64
	dlq              chan dlqCall
	acks             atomic.Int32
	nacks            atomic.Int32
}

func (q *reliabilityQueue) Enqueue(context.Context, string, queue.JSONPayload, time.Time, string) (*int64, error) {
	return new(int64(1)), nil
}

func (q *reliabilityQueue) Claim(context.Context, []string, int) ([]queue.Claimed, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	claimed := q.claimed
	q.claimed = nil
	return claimed, nil
}

func (q *reliabilityQueue) StartHeartbeat(ctx context.Context, taskID int64, _ uuid.UUID, _ chan struct{}) {
	if q.heartbeatStarted != nil {
		q.heartbeatStarted <- taskID
	}
	<-ctx.Done()
}

func (q *reliabilityQueue) Ack(context.Context, int64, uuid.UUID) (bool, error) {
	q.acks.Add(1)
	return true, nil
}

func (q *reliabilityQueue) Nack(context.Context, int64, uuid.UUID, string, time.Duration) (bool, error) {
	q.nacks.Add(1)
	return true, nil
}

func (q *reliabilityQueue) MoveToDLQ(_ context.Context, taskID int64, lease uuid.UUID, errText string) (bool, error) {
	if q.dlq != nil {
		q.dlq <- dlqCall{taskID: taskID, lease: lease, errText: errText}
	}
	return true, nil
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
