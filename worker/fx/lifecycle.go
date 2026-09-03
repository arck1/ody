// Package workerfx integrates a worker runner with an Uber Fx lifecycle.
// The core worker package does not depend on Fx and can still be run directly.
package workerfx

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/fx"
)

// Runner is implemented by worker.Worker and can also be used by custom
// execution loops.
type Runner interface {
	Run(context.Context) error
}

// Lifecycle owns one background run of a Runner.
type Lifecycle struct {
	runner Runner

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan error
}

// New creates an Fx lifecycle adapter for runner.
func New(runner Runner) (*Lifecycle, error) {
	if runner == nil {
		return nil, errors.New("worker fx runner is nil")
	}
	return &Lifecycle{runner: runner}, nil
}

// Init registers worker start and graceful stop hooks with Fx.
func (l *Lifecycle) Init(fxLifecycle fx.Lifecycle) {
	fxLifecycle.Append(fx.Hook{
		OnStart: l.start,
		OnStop:  l.stop,
	})
}

func (l *Lifecycle) start(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done != nil {
		return errors.New("worker fx lifecycle already started")
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	l.cancel = cancel
	l.done = done
	go func() {
		err := l.runner.Run(runCtx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		done <- err
		close(done)
	}()
	return nil
}

func (l *Lifecycle) stop(ctx context.Context) error {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if done == nil {
		return nil
	}
	if cancel != nil {
		cancel()
	}

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("worker stopped: %w", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop worker: %w", ctx.Err())
	}
}
