package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"ody/execution"
	"ody/task"
	"ody/worker"
)

type flowInput struct {
	Number int `json:"number"`
}
type sumInput struct{ Left, Right int }

func TestPipelineStoresOutputsAndFeedsFanIn(t *testing.T) {
	double := task.New[int, int]("math.double")
	increment := task.New[int, int]("math.increment", task.WithMaxAttempts(2), task.WithRetryPolicy(func(int) time.Duration { return time.Millisecond }))
	sum := task.New[sumInput, int]("math.sum")
	var incrementCalls atomic.Int32
	module, err := task.NewModule("math",
		task.Handle(double, func(_ context.Context, message task.Message[int]) (int, error) { return message.Input * 2, nil }),
		task.Handle(increment, func(_ context.Context, message task.Message[int]) (int, error) {
			if incrementCalls.Add(1) == 1 {
				return 0, assertError("temporary")
			}
			return message.Input + 1, nil
		}),
		task.Handle(sum, func(_ context.Context, message task.Message[sumInput]) (int, error) {
			return message.Input.Left + message.Input.Right, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := task.NewRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	flow := New[flowInput]("math-flow", 1)
	left := Start(flow, "double", double, func(input flowInput) int { return input.Number })
	right := Start(flow, "increment", increment, func(input flowInput) int { return input.Number })
	total := Join2(flow, left, right, "sum", sum, func(a, b int) sumInput { return sumInput{Left: a, Right: b} })
	pipelines, err := NewRegistry(flow)
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewMemoryStore()
	engine, err := NewEngine(store, pipelines)
	if err != nil {
		t.Fatal(err)
	}
	run, err := Run(context.Background(), engine, flow, flowInput{Number: 4})
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(store, tasks, engine, nil, worker.Options{Concurrency: 2, PollInterval: time.Millisecond, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	eventually(t, 2*time.Second, func() bool {
		current, getErr := store.GetPipelineRun(context.Background(), run.ID)
		return getErr == nil && current.Status == execution.RunSucceeded
	})
	cancel()
	<-done
	value, err := Output(context.Background(), store, run.ID, total)
	if err != nil {
		t.Fatal(err)
	}
	if value != 13 {
		t.Fatalf("pipeline output = %d, want 13", value)
	}
	executions, _ := store.ListRunExecutions(context.Background(), run.ID)
	if len(executions) != 3 {
		t.Fatalf("executions = %d, want 3", len(executions))
	}
	var retryEvents int
	for _, item := range executions {
		events, _ := store.Events(context.Background(), item.ID)
		for _, event := range events {
			if event.Type == execution.EventRetried {
				retryEvents++
			}
		}
	}
	if retryEvents != 1 {
		t.Fatalf("retry events = %d, want 1", retryEvents)
	}
}

func TestReconcileSchedulesSuccessorAfterCompletionGap(t *testing.T) {
	firstTask := task.New[int, int]("gap.first")
	secondTask := task.New[int, int]("gap.second")
	flow := New[int]("gap-flow", 1)
	first := Start(flow, "first", firstTask, func(value int) int { return value })
	Then(flow, first, "second", secondTask, func(value int) int { return value + 1 })
	registry, _ := NewRegistry(flow)
	store := execution.NewMemoryStore()
	engine, _ := NewEngine(store, registry)
	run, err := Run(context.Background(), engine, flow, 10)
	if err != nil {
		t.Fatal(err)
	}
	claimed, _ := store.Claim(context.Background(), "worker", []execution.TaskKey{{Name: "gap.first", Version: 1}}, 1, time.Minute)
	if err = store.Succeed(context.Background(), claimed[0].ID, claimed[0].LeaseToken, []byte(`10`)); err != nil {
		t.Fatal(err)
	}
	items, _ := store.ListRunExecutions(context.Background(), run.ID)
	if len(items) != 1 {
		t.Fatalf("expected simulated advancement gap")
	}
	if err = engine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	items, _ = store.ListRunExecutions(context.Background(), run.ID)
	if len(items) != 2 {
		t.Fatalf("reconcile created %d executions", len(items))
	}
}

func TestCancelPipelineInvalidatesPendingNodes(t *testing.T) {
	definition := task.New[int, int]("cancel.task")
	flow := New[int]("cancel-flow", 1)
	Start(flow, "work", definition, func(value int) int { return value })
	registry, _ := NewRegistry(flow)
	store := execution.NewMemoryStore()
	engine, _ := NewEngine(store, registry)
	run, _ := Run(context.Background(), engine, flow, 1)
	if err := engine.Cancel(context.Background(), run.ID, "requested"); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := engine.Inspect(context.Background(), run.ID)
	if snapshot.Run.Status != execution.RunCancelled || snapshot.Executions[0].Status != execution.StatusCancelled {
		t.Fatalf("unexpected cancellation snapshot: %+v", snapshot)
	}
}

func TestRestartExecutionRebuildsPipelineDescendants(t *testing.T) {
	var source atomic.Int32
	source.Store(2)
	firstTask := task.New[int, int]("restart.first")
	secondTask := task.New[int, int]("restart.second")
	module, _ := task.NewModule("restart",
		task.Handle(firstTask, func(context.Context, task.Message[int]) (int, error) {
			return int(source.Load()), nil
		}),
		task.Handle(secondTask, func(_ context.Context, message task.Message[int]) (int, error) {
			return message.Input * 10, nil
		}),
	)
	tasks, _ := task.NewRegistry(module)
	flow := New[int]("restart-flow", 1)
	first := Start(flow, "first", firstTask, func(value int) int { return value })
	second := Then(flow, first, "second", secondTask, func(value int) int { return value })
	pipelines, _ := NewRegistry(flow)
	store := execution.NewMemoryStore()
	engine, _ := NewEngine(store, pipelines)
	run, err := Run(context.Background(), engine, flow, 1)
	if err != nil {
		t.Fatal(err)
	}
	runner, _ := worker.New(store, tasks, engine, nil, worker.Options{PollInterval: time.Millisecond, LeaseDuration: time.Second})
	runWorkerUntil(t, runner, func() bool {
		current, getErr := store.GetPipelineRun(context.Background(), run.ID)
		return getErr == nil && current.Status == execution.RunSucceeded
	})
	items, _ := store.ListRunExecutions(context.Background(), run.ID)
	rootID := items[0].ID

	source.Store(3)
	if _, err = engine.RestartExecution(context.Background(), rootID); err != nil {
		t.Fatal(err)
	}
	items, _ = store.ListRunExecutions(context.Background(), run.ID)
	if items[0].Status != execution.StatusPending || items[1].Status != execution.StatusBlocked {
		t.Fatalf("restart states = %s -> %s", items[0].Status, items[1].Status)
	}
	runWorkerUntil(t, runner, func() bool {
		current, getErr := store.GetPipelineRun(context.Background(), run.ID)
		return getErr == nil && current.Status == execution.RunSucceeded
	})
	value, err := Output(context.Background(), store, run.ID, second)
	if err != nil {
		t.Fatal(err)
	}
	if value != 30 {
		t.Fatalf("rebuilt output = %d, want 30", value)
	}
}

type assertError string

func (e assertError) Error() string { return string(e) }

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met")
}

func runWorkerUntil(t *testing.T, runner *worker.Worker, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = runner.Run(ctx)
		close(done)
	}()
	eventually(t, 2*time.Second, condition)
	cancel()
	<-done
}
