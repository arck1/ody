package ody

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/arck1/ody/execution"
	"github.com/arck1/ody/task"
	"github.com/arck1/ody/worker"
)

func TestAppRunsTypedTaskStandalone(t *testing.T) {
	definition := task.New[int, int]("app.double")
	module, err := task.NewModule("app", task.Handle(definition, func(_ context.Context, message task.Message[int]) (int, error) {
		return message.Input * 2, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewMemoryStore()
	app, err := New(store, Tasks(module), WithWorker(workerOptions()))
	if err != nil {
		t.Fatal(err)
	}
	created, err := definition.Enqueue(context.Background(), app, 21)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	eventually(t, func() bool {
		item, getErr := store.GetExecution(context.Background(), created.ID)
		return getErr == nil && item.Status == execution.StatusSucceeded
	})
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v", err)
	}
}

func TestFxModuleRunsSameApp(t *testing.T) {
	definition := task.New[int, int]("fx.double")
	module, _ := task.NewModule("fx", task.Handle(definition, func(_ context.Context, message task.Message[int]) (int, error) {
		return message.Input * 2, nil
	}))
	store := execution.NewMemoryStore()
	var app *App
	fxApp := fx.New(
		fx.Supply(fx.Annotate(store, fx.As(new(execution.Store)))),
		FxModule(Tasks(module), WithWorker(workerOptions())),
		fx.Populate(&app),
		fx.NopLogger,
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fxApp.Start(ctx); err != nil {
		t.Fatal(err)
	}
	created, err := definition.Enqueue(ctx, app, 10)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		item, getErr := store.GetExecution(context.Background(), created.ID)
		return getErr == nil && item.Status == execution.StatusSucceeded
	})
	if err = fxApp.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func workerOptions() worker.Options {
	return worker.Options{PollInterval: time.Millisecond, LeaseDuration: time.Second, HeartbeatInterval: time.Millisecond * 100}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met")
}
