package workerfx

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx"
)

func TestLifecycleStartsAndStopsRunner(t *testing.T) {
	runner := &testRunner{started: make(chan struct{})}
	lifecycle, err := New(runner)
	if err != nil {
		t.Fatal(err)
	}
	app := fx.New(fx.Invoke(lifecycle.Init), fx.NopLogger)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runner.started:
	case <-ctx.Done():
		t.Fatal("runner was not started")
	}
	if err = app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if !runner.stopped.Load() {
		t.Fatal("runner was not stopped before Fx shutdown completed")
	}
}

func TestLifecycleReturnsRunnerFailureOnStop(t *testing.T) {
	want := errors.New("runner failed")
	lifecycle, err := New(runnerFunc(func(context.Context) error { return want }))
	if err != nil {
		t.Fatal(err)
	}
	app := fx.New(fx.Invoke(lifecycle.Init), fx.NopLogger)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err = app.Stop(ctx); !errors.Is(err, want) {
		t.Fatalf("Stop error = %v, want %v", err, want)
	}
}

func TestNewRejectsNilRunner(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("expected nil runner error")
	}
}

type testRunner struct {
	started chan struct{}
	stopped atomic.Bool
}

func (r *testRunner) Run(ctx context.Context) error {
	close(r.started)
	<-ctx.Done()
	r.stopped.Store(true)
	return ctx.Err()
}

type runnerFunc func(context.Context) error

func (f runnerFunc) Run(ctx context.Context) error { return f(ctx) }
