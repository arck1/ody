package functional_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"
	"schedulor"
	"schedulor/execution"
	"schedulor/pipeline"
	"schedulor/task"
	"schedulor/worker"
	workerfx "schedulor/worker/fx"
)

func TestStandaloneTaskPersistsResultAndHistory(t *testing.T) {
	type input struct{ Name string }
	type output struct{ Greeting string }

	definition := task.New[input, output]("functional.greet")
	module, err := task.NewModule("greeting", task.Handle(definition, func(_ context.Context, message task.Message[input]) (output, error) {
		return output{Greeting: "Hello, " + message.Input.Name}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := task.NewRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewMemoryStore()
	created, err := definition.Enqueue(context.Background(), store, input{Name: "Ada"}, task.WithIdempotencyKey("greet:ada"))
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := definition.Enqueue(context.Background(), store, input{Name: "Ada"}, task.WithIdempotencyKey("greet:ada"))
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.ID != created.ID {
		t.Fatalf("idempotent enqueue created %s, want %s", duplicate.ID, created.ID)
	}

	runner, err := worker.New(store, registry, nil, nil, fastWorkerOptions())
	if err != nil {
		t.Fatal(err)
	}
	stop := startWorker(t, runner)
	defer stop()

	completed := awaitExecution(t, store, created.ID, execution.StatusSucceeded)
	var result output
	if err = json.Unmarshal(completed.Output, &result); err != nil {
		t.Fatal(err)
	}
	if result.Greeting != "Hello, Ada" {
		t.Fatalf("stored output = %#v", result)
	}
	assertEventTypes(t, store, created.ID,
		execution.EventCreated,
		execution.EventStarted,
		execution.EventSucceeded,
	)
}

func TestDelayedTaskRetriesThenSucceeds(t *testing.T) {
	definition := task.New[int, int](
		"functional.delayed-retry",
		task.WithMaxAttempts(3),
		task.WithRetryPolicy(func(int) time.Duration { return time.Hour }),
	)
	var calls atomic.Int32
	var callMu sync.Mutex
	var callTimes []time.Time
	module, err := task.NewModule("retry", task.Handle(definition, func(_ context.Context, message task.Message[int]) (int, error) {
		callMu.Lock()
		callTimes = append(callTimes, time.Now())
		callMu.Unlock()
		if calls.Add(1) == 1 {
			return 0, task.RetryAfter(errors.New("temporary outage"), 40*time.Millisecond)
		}
		return message.Input * 2, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := task.NewRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewMemoryStore()
	availableAt := time.Now().Add(50 * time.Millisecond)
	created, err := definition.Enqueue(context.Background(), store, 21, task.WithAvailableAt(availableAt))
	if err != nil {
		t.Fatal(err)
	}
	runner, err := worker.New(store, registry, nil, nil, fastWorkerOptions())
	if err != nil {
		t.Fatal(err)
	}
	stop := startWorker(t, runner)
	defer stop()

	completed := awaitExecution(t, store, created.ID, execution.StatusSucceeded)
	if completed.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", completed.Attempt)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want 2", calls.Load())
	}
	callMu.Lock()
	times := append([]time.Time(nil), callTimes...)
	callMu.Unlock()
	if times[0].Before(availableAt) {
		t.Fatalf("task ran at %v before available_at %v", times[0], availableAt)
	}
	if delay := times[1].Sub(times[0]); delay < 30*time.Millisecond {
		t.Fatalf("retry delay = %v, expected RetryAfter delay", delay)
	}
	assertEventTypes(t, store, created.ID,
		execution.EventCreated,
		execution.EventStarted,
		execution.EventRetried,
		execution.EventStarted,
		execution.EventSucceeded,
	)
}

func TestFanOutFanInPipelineUsesStoredOutputs(t *testing.T) {
	type order struct {
		Price    int
		Quantity int
		Discount int
	}
	type totalInput struct{ Subtotal, Discount int }

	subtotalTask := task.New[order, int]("functional.subtotal")
	discountTask := task.New[order, int]("functional.discount")
	totalTask := task.New[totalInput, int]("functional.total")
	module, err := task.NewModule("checkout",
		task.Handle(subtotalTask, func(_ context.Context, message task.Message[order]) (int, error) {
			return message.Input.Price * message.Input.Quantity, nil
		}),
		task.Handle(discountTask, func(_ context.Context, message task.Message[order]) (int, error) {
			return message.Input.Discount, nil
		}),
		task.Handle(totalTask, func(_ context.Context, message task.Message[totalInput]) (int, error) {
			return message.Input.Subtotal - message.Input.Discount, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := task.NewRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	flow := pipeline.New[order]("functional-checkout", 1)
	subtotal := pipeline.Start(flow, "subtotal", subtotalTask, func(value order) order { return value })
	discount := pipeline.Start(flow, "discount", discountTask, func(value order) order { return value })
	total := pipeline.Join2(flow, subtotal, discount, "total", totalTask, func(left, right int) totalInput {
		return totalInput{Subtotal: left, Discount: right}
	})
	pipelines, err := pipeline.NewRegistry(flow)
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewMemoryStore()
	engine, err := pipeline.NewEngine(store, pipelines)
	if err != nil {
		t.Fatal(err)
	}
	run, err := pipeline.Run(context.Background(), engine, flow, order{Price: 10, Quantity: 4, Discount: 3})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := worker.New(store, tasks, engine, nil, worker.Options{
		Concurrency:       2,
		PollInterval:      time.Millisecond,
		LeaseDuration:     time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := startWorker(t, runner)
	defer stop()

	awaitPipeline(t, store, run.ID, execution.RunSucceeded)
	result, err := pipeline.Output(context.Background(), store, run.ID, total)
	if err != nil {
		t.Fatal(err)
	}
	if result != 37 {
		t.Fatalf("pipeline output = %d, want 37", result)
	}
	snapshot, err := engine.Inspect(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Executions) != 3 {
		t.Fatalf("pipeline executions = %d, want 3", len(snapshot.Executions))
	}
}

func TestPermanentPipelineFailureDoesNotScheduleDownstream(t *testing.T) {
	load := task.New[string, string]("functional.load")
	validate := task.New[string, string]("functional.validate")
	publish := task.New[string, string]("functional.publish")
	var published atomic.Bool
	module, err := task.NewModule("publication",
		task.Handle(load, func(_ context.Context, message task.Message[string]) (string, error) {
			return "loaded:" + message.Input, nil
		}),
		task.Handle(validate, func(context.Context, task.Message[string]) (string, error) {
			return "", task.Permanent(errors.New("document is invalid"))
		}),
		task.Handle(publish, func(_ context.Context, message task.Message[string]) (string, error) {
			published.Store(true)
			return message.Input, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := task.NewRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	flow := pipeline.New[string]("functional-publication", 1)
	loaded := pipeline.Start(flow, "load", load, func(input string) string { return input })
	validated := pipeline.Then(flow, loaded, "validate", validate, func(output string) string { return output })
	pipeline.Then(flow, validated, "publish", publish, func(output string) string { return output })
	pipelines, err := pipeline.NewRegistry(flow)
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewMemoryStore()
	engine, err := pipeline.NewEngine(store, pipelines)
	if err != nil {
		t.Fatal(err)
	}
	run, err := pipeline.Run(context.Background(), engine, flow, "document")
	if err != nil {
		t.Fatal(err)
	}
	runner, err := worker.New(store, tasks, engine, nil, fastWorkerOptions())
	if err != nil {
		t.Fatal(err)
	}
	stop := startWorker(t, runner)
	defer stop()

	failed := awaitPipeline(t, store, run.ID, execution.RunFailed)
	if !strings.Contains(failed.Error, "validate") || !strings.Contains(failed.Error, "document is invalid") {
		t.Fatalf("pipeline error = %q", failed.Error)
	}
	if published.Load() {
		t.Fatal("downstream publish handler was called")
	}
	snapshot, err := engine.Inspect(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Executions) != 2 {
		t.Fatalf("scheduled executions = %d, want only load and validate", len(snapshot.Executions))
	}
}

func TestCancelledPipelineNeverRunsPendingTask(t *testing.T) {
	definition := task.New[string, string]("functional.cancel")
	var called atomic.Bool
	module, err := task.NewModule("cancellation", task.Handle(definition, func(_ context.Context, message task.Message[string]) (string, error) {
		called.Store(true)
		return message.Input, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := task.NewRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	flow := pipeline.New[string]("functional-cancellation", 1)
	pipeline.Start(flow, "work", definition, func(input string) string { return input })
	pipelines, err := pipeline.NewRegistry(flow)
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewMemoryStore()
	engine, err := pipeline.NewEngine(store, pipelines)
	if err != nil {
		t.Fatal(err)
	}
	run, err := pipeline.Run(context.Background(), engine, flow, "payload")
	if err != nil {
		t.Fatal(err)
	}
	if err = engine.Cancel(context.Background(), run.ID, "user request"); err != nil {
		t.Fatal(err)
	}
	runner, err := worker.New(store, tasks, engine, nil, fastWorkerOptions())
	if err != nil {
		t.Fatal(err)
	}
	stop := startWorker(t, runner)
	time.Sleep(20 * time.Millisecond)
	stop()

	if called.Load() {
		t.Fatal("cancelled task handler was called")
	}
	snapshot, err := engine.Inspect(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Run.Status != execution.RunCancelled {
		t.Fatalf("pipeline status = %q, want cancelled", snapshot.Run.Status)
	}
	if len(snapshot.Executions) != 1 || snapshot.Executions[0].Status != execution.StatusCancelled {
		t.Fatalf("unexpected execution state: %#v", snapshot.Executions)
	}
	assertEventTypes(t, store, snapshot.Executions[0].ID,
		execution.EventCreated,
		execution.EventCancelled,
	)
}

func TestWorkerRunsThroughFxLifecycle(t *testing.T) {
	definition := task.New[int, int]("functional.fx")
	module, err := task.NewModule("fx", task.Handle(definition, func(_ context.Context, message task.Message[int]) (int, error) {
		return message.Input + 1, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := task.NewRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewMemoryStore()
	created, err := definition.Enqueue(context.Background(), store, 41)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := worker.New(store, registry, nil, nil, fastWorkerOptions())
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, err := workerfx.New(runner)
	if err != nil {
		t.Fatal(err)
	}
	app, err := schedulor.NewFxApp(schedulor.FxAppOptions{
		Components: []schedulor.FxLifecycleComponent{lifecycle},
		Options:    []fx.Option{fx.NopLogger},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	awaitExecution(t, store, created.ID, execution.StatusSucceeded)
	if err = app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func fastWorkerOptions() worker.Options {
	return worker.Options{
		PollInterval:      time.Millisecond,
		LeaseDuration:     time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
	}
}

func startWorker(t *testing.T, runner *worker.Worker) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("worker stopped with error: %v", err)
				}
			case <-time.After(time.Second):
				t.Error("worker did not stop")
			}
		})
	}
}

func awaitExecution(t *testing.T, store execution.Store, id uuid.UUID, status execution.Status) execution.Execution {
	t.Helper()
	var result execution.Execution
	eventually(t, 2*time.Second, func() bool {
		item, err := store.GetExecution(context.Background(), id)
		result = item
		return err == nil && item.Status == status
	})
	return result
}

func awaitPipeline(t *testing.T, store execution.Store, id uuid.UUID, status execution.RunStatus) execution.PipelineRun {
	t.Helper()
	var result execution.PipelineRun
	eventually(t, 2*time.Second, func() bool {
		run, err := store.GetPipelineRun(context.Background(), id)
		result = run
		return err == nil && run.Status == status
	})
	return result
}

func assertEventTypes(t *testing.T, store execution.Store, id uuid.UUID, expected ...execution.EventType) {
	t.Helper()
	events, err := store.Events(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != len(expected) {
		t.Fatalf("event count = %d, want %d: %#v", len(events), len(expected), events)
	}
	for index := range expected {
		if events[index].Type != expected[index] {
			t.Fatalf("event %d = %q, want %q", index, events[index].Type, expected[index])
		}
	}
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
