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

// Persistence is the minimal storage capability required by a worker.
type Persistence interface {
	execution.Queue
	GetExecution(context.Context, uuid.UUID) (execution.Execution, error)
}

var ErrShutdownTimeout = errors.New("worker shutdown grace period elapsed")

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
	// MaxConsecutiveErrors stops a polling loop after this many infrastructure failures. Zero keeps
	// retrying forever while reporting degraded health.
	MaxConsecutiveErrors int
	// ShutdownGracePeriod bounds waiting for handlers that ignore cancellation. The lease remains
	// the recovery boundary when this timeout elapses.
	ShutdownGracePeriod time.Duration
}

type Operation string

const (
	OperationClaim     Operation = "claim"
	OperationReap      Operation = "reap"
	OperationAdvance   Operation = "advance"
	OperationReconcile Operation = "reconcile"
	OperationReload    Operation = "reload"
)

// Observer receives completed worker decisions. Implementations must avoid blocking the worker.
type Observer interface {
	// Transition reports the intended status and the handler or persistence error that caused it.
	Transition(context.Context, execution.Execution, execution.Status, error)
	InfrastructureError(context.Context, Operation, error)
}
type nopObserver struct{}

func (nopObserver) Transition(context.Context, execution.Execution, execution.Status, error) {}
func (nopObserver) InfrastructureError(context.Context, Operation, error)                    {}

type HealthStatus string

const (
	HealthReady    HealthStatus = "ready"
	HealthDegraded HealthStatus = "degraded"
)

type Health struct {
	Status              HealthStatus
	ConsecutiveFailures int
	LastError           string
	LastFailureAt       time.Time
}

type Worker struct {
	store    Persistence
	registry *task.Registry
	advancer Advancer
	observer Observer
	options  Options
	healthMu sync.RWMutex
	health   Health
	handlers sync.WaitGroup
}

func New(store Persistence, registry *task.Registry, advancer Advancer, observer Observer, options Options) (*Worker, error) {
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
	if options.ShutdownGracePeriod <= 0 {
		options.ShutdownGracePeriod = 30 * time.Second
	}
	if observer == nil {
		observer = nopObserver{}
	}
	return &Worker{
		store: store, registry: registry, advancer: advancer, observer: observer, options: options,
		health: Health{Status: HealthReady},
	}, nil
}

// Run blocks until ctx is cancelled and drains all worker goroutines.
func (w *Worker) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var group sync.WaitGroup
	for index := 0; index < w.options.Concurrency; index++ {
		group.Go(func() {
			if err := w.loop(runCtx); err != nil {
				cancel(err)
			}
		})
	}
	group.Wait()
	if !w.waitForHandlers() {
		return ErrShutdownTimeout
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return context.Cause(runCtx)
}

func (w *Worker) waitForHandlers() bool {
	done := make(chan struct{})
	go func() {
		w.handlers.Wait()
		close(done)
	}()
	timer := time.NewTimer(w.options.ShutdownGracePeriod)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (w *Worker) loop(ctx context.Context) error {
	consecutiveErrors := 0
	for ctx.Err() == nil {
		hadInfrastructureError := false
		// Reaping before claiming prevents exhausted crashed deliveries from blocking the queue head.
		runs, err := w.store.ReapExpired(ctx)
		if err != nil {
			hadInfrastructureError = true
			if w.infrastructureFailure(ctx, OperationReap, err, &consecutiveErrors) {
				return fmt.Errorf("reap expired executions: %w", err)
			}
			if !wait(ctx, w.options.PollInterval) {
				break
			}
			continue
		}
		for _, runID := range runs {
			if w.advancer != nil {
				if err = w.advancer.Advance(ctx, runID); err != nil {
					hadInfrastructureError = true
					if w.infrastructureFailure(ctx, OperationAdvance, err, &consecutiveErrors) {
						return fmt.Errorf("advance reaped pipeline: %w", err)
					}
				}
			}
		}
		items, err := w.store.Claim(ctx, w.options.ID, w.registry.Keys(), 1, w.options.LeaseDuration)
		if err != nil {
			hadInfrastructureError = true
			if w.infrastructureFailure(ctx, OperationClaim, err, &consecutiveErrors) {
				return fmt.Errorf("claim execution: %w", err)
			}
		}
		if err != nil || len(items) == 0 {
			if reconcile, ok := w.advancer.(reconciler); ok {
				if reconcileErr := reconcile.Reconcile(ctx); reconcileErr != nil {
					hadInfrastructureError = true
					if w.infrastructureFailure(ctx, OperationReconcile, reconcileErr, &consecutiveErrors) {
						return fmt.Errorf("reconcile pipelines: %w", reconcileErr)
					}
				}
			}
			if !hadInfrastructureError {
				w.healthy(&consecutiveErrors)
			}
			if !wait(ctx, w.options.PollInterval) {
				break
			}
			continue
		}
		if !hadInfrastructureError {
			w.healthy(&consecutiveErrors)
		}
		w.execute(ctx, items[0])
	}
	return nil
}

func (w *Worker) infrastructureFailure(ctx context.Context, operation Operation, err error, consecutive *int) bool {
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return false
	}
	(*consecutive)++
	w.healthMu.Lock()
	w.health = Health{
		Status: HealthDegraded, ConsecutiveFailures: *consecutive,
		LastError: err.Error(), LastFailureAt: time.Now().UTC(),
	}
	w.healthMu.Unlock()
	w.observer.InfrastructureError(ctx, operation, err)
	return w.options.MaxConsecutiveErrors > 0 && *consecutive >= w.options.MaxConsecutiveErrors
}

func (w *Worker) healthy(consecutive *int) {
	*consecutive = 0
	w.healthMu.Lock()
	w.health = Health{Status: HealthReady}
	w.healthMu.Unlock()
}

// Health returns the latest state of worker infrastructure operations.
func (w *Worker) Health() Health {
	w.healthMu.RLock()
	defer w.healthMu.RUnlock()
	return w.health
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
	w.handlers.Add(1)
	go func() {
		defer w.handlers.Done()
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
	if err := w.store.Retry(ctx, item.ID, item.LeaseToken, handlerErr.Error(), delay); err != nil {
		w.observer.Transition(ctx, item, execution.StatusRunning, err)
		return
	}
	updated, err := w.store.GetExecution(ctx, item.ID)
	if err != nil {
		w.observer.InfrastructureError(ctx, OperationReload, err)
		w.observer.Transition(ctx, item, execution.StatusRetry, handlerErr)
		return
	}
	w.observer.Transition(ctx, updated, updated.Status, handlerErr)
	if updated.Status == execution.StatusFailed {
		w.advance(ctx, updated)
	}
}

func (w *Worker) advance(ctx context.Context, item execution.Execution) {
	if w.advancer != nil && item.PipelineRunID != nil {
		if err := w.advancer.Advance(ctx, *item.PipelineRunID); err != nil {
			w.observer.InfrastructureError(ctx, OperationAdvance, err)
		}
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
