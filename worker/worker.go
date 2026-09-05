// Package worker executes registered tasks from an execution Store.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"schedulor/execution"
	"schedulor/task"
)

type Advancer interface {
	// Advance reconciles a pipeline after one of its node executions changes state.
	Advance(context.Context, uuid.UUID) error
}

type reconciler interface{ Reconcile(context.Context) error }

type Options struct {
	// ID is persisted as LeaseOwner and should identify one worker process.
	ID string
	// Concurrency is the number of independent claim-and-execute loops.
	Concurrency int
	// PollInterval is used only when no execution could be claimed or the Store returned an error.
	PollInterval time.Duration
	// LeaseDuration controls when another worker may reclaim an abandoned execution.
	LeaseDuration time.Duration
	// HeartbeatInterval controls lease renewal and must be shorter than LeaseDuration.
	HeartbeatInterval time.Duration
}

// Observer receives completed worker decisions. Implementations must avoid blocking the worker.
type Observer interface {
	// Transition reports the intended status and the handler or persistence error that caused it.
	Transition(context.Context, execution.Execution, execution.Status, error)
}
type nopObserver struct{}

func (nopObserver) Transition(context.Context, execution.Execution, execution.Status, error) {}

type Worker struct {
	store    execution.Store
	registry *task.Registry
	advancer Advancer
	observer Observer
	options  Options
}

func New(store execution.Store, registry *task.Registry, advancer Advancer, observer Observer, options Options) (*Worker, error) {
	if store == nil {
		return nil, errors.New("worker store is nil")
	}
	if registry == nil {
		return nil, errors.New("task registry is nil")
	}
	if options.ID == "" {
		options.ID = uuid.NewString()
	}
	if options.Concurrency <= 0 {
		options.Concurrency = 1
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 100 * time.Millisecond
	}
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = 30 * time.Second
	}
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = options.LeaseDuration / 3
	}
	if options.HeartbeatInterval >= options.LeaseDuration {
		return nil, errors.New("heartbeat interval must be shorter than lease duration")
	}
	if observer == nil {
		observer = nopObserver{}
	}
	return &Worker{store: store, registry: registry, advancer: advancer, observer: observer, options: options}, nil
}

// Run blocks until ctx is cancelled and drains all worker goroutines.
func (w *Worker) Run(ctx context.Context) error {
	var group sync.WaitGroup
	for index := 0; index < w.options.Concurrency; index++ {
		group.Go(func() { w.loop(ctx) })
	}
	group.Wait()
	return ctx.Err()
}

func (w *Worker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		// Reaping before claiming prevents exhausted crashed deliveries from blocking the queue head.
		runs, _ := w.store.ReapExpired(ctx)
		for _, runID := range runs {
			if w.advancer != nil {
				_ = w.advancer.Advance(ctx, runID)
			}
		}
		items, err := w.store.Claim(ctx, w.options.ID, w.registry.Names(), 1, w.options.LeaseDuration)
		if err != nil || len(items) == 0 {
			if reconcile, ok := w.advancer.(reconciler); ok {
				_ = reconcile.Reconcile(ctx)
			}
			if !wait(ctx, w.options.PollInterval) {
				return
			}
			continue
		}
		w.execute(ctx, items[0])
	}
}

type result struct {
	output json.RawMessage
	err    error
}

func (w *Worker) execute(workerCtx context.Context, item execution.Execution) {
	_, timeout, retryPolicy, registered := w.registry.Policy(item.TaskName, item.TaskVersion)
	var handlerCtx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		handlerCtx, cancel = context.WithTimeout(workerCtx, timeout)
	} else {
		handlerCtx, cancel = context.WithCancel(workerCtx)
	}
	defer cancel()
	// The buffer lets a late handler finish without blocking while the worker has already resolved a
	// timeout or shutdown. Go cannot forcibly stop a handler; it must still honor its context.
	resultCh := make(chan result, 1)
	go func() {
		output, err := w.registry.Execute(handlerCtx, item)
		resultCh <- result{output: output, err: err}
	}()
	ticker := time.NewTicker(w.options.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-workerCtx.Done():
			return
		case <-handlerCtx.Done():
			w.resolve(workerCtx, item, nil, handlerCtx.Err(), retryPolicy, registered)
			return
		case value := <-resultCh:
			w.resolve(workerCtx, item, value.output, value.err, retryPolicy, registered)
			return
		case <-ticker.C:
			if err := w.store.Heartbeat(workerCtx, item.ID, item.LeaseToken, w.options.LeaseDuration); err != nil {
				cancel()
				w.observer.Transition(workerCtx, item, execution.StatusRunning, fmt.Errorf("heartbeat: %w", err))
				return
			}
		}
	}
}

func (w *Worker) resolve(ctx context.Context, item execution.Execution, output json.RawMessage, handlerErr error, retryPolicy task.RetryPolicy, registered bool) {
	if handlerErr == nil {
		if err := w.store.Succeed(ctx, item.ID, item.LeaseToken, output); err != nil {
			w.observer.Transition(ctx, item, execution.StatusRunning, err)
			return
		}
		w.observer.Transition(ctx, item, execution.StatusSucceeded, nil)
		w.advance(ctx, item)
		return
	}
	if !registered || task.IsPermanent(handlerErr) {
		if err := w.store.Fail(ctx, item.ID, item.LeaseToken, handlerErr.Error()); err != nil {
			w.observer.Transition(ctx, item, execution.StatusRunning, err)
			return
		}
		w.observer.Transition(ctx, item, execution.StatusFailed, handlerErr)
		w.advance(ctx, item)
		return
	}
	delay, ok := task.RetryDelay(handlerErr)
	if !ok {
		delay = retryPolicy(item.Attempt)
	}
	if err := w.store.Retry(ctx, item.ID, item.LeaseToken, handlerErr.Error(), time.Now().UTC().Add(delay)); err != nil {
		w.observer.Transition(ctx, item, execution.StatusRunning, err)
		return
	}
	updated, _ := w.store.GetExecution(ctx, item.ID)
	w.observer.Transition(ctx, updated, updated.Status, handlerErr)
	if updated.Status == execution.StatusFailed {
		w.advance(ctx, updated)
	}
}

func (w *Worker) advance(ctx context.Context, item execution.Execution) {
	if w.advancer != nil && item.PipelineRunID != nil {
		_ = w.advancer.Advance(ctx, *item.PipelineRunID)
	}
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
