// Package ody provides the high-level runtime for typed durable tasks and pipelines.
package ody

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/arck1/ody/execution"
	"github.com/arck1/ody/pipeline"
	"github.com/arck1/ody/schedule"
	"github.com/arck1/ody/task"
	"github.com/arck1/ody/worker"
)

type Option func(*configuration)

type configuration struct {
	modules   []task.Module
	pipelines []pipeline.RegisteredDefinition
	observer  worker.Observer
	worker    worker.Options
	schedules []schedule.Definition
	scheduler schedule.Options
}

// Tasks registers application task modules in the runtime.
func Tasks(modules ...task.Module) Option {
	return func(config *configuration) { config.modules = append(config.modules, modules...) }
}

// Pipelines registers durable DAG definitions available for new and resumed runs.
func Pipelines(definitions ...pipeline.RegisteredDefinition) Option {
	return func(config *configuration) { config.pipelines = append(config.pipelines, definitions...) }
}

// Observe attaches a non-blocking worker observer such as Prometheus metrics or a logger adapter.
func Observe(observer worker.Observer) Option {
	return func(config *configuration) { config.observer = observer }
}

// WithWorker configures delivery concurrency, polling, leases, and failure handling.
func WithWorker(options worker.Options) Option {
	return func(config *configuration) { config.worker = options }
}

// Schedules registers cron triggers that enqueue through the same durable Store.
func Schedules(definitions ...schedule.Definition) Option {
	return func(config *configuration) { config.schedules = append(config.schedules, definitions...) }
}

func WithScheduler(options schedule.Options) Option {
	return func(config *configuration) { config.scheduler = options }
}

// App is the common standalone and Fx runtime. It is also a task.ExecutionCreator, so typed task
// definitions can enqueue directly through it.
type App struct {
	store     execution.Store
	worker    *worker.Worker
	pipelines *pipeline.Engine
	scheduler *schedule.Scheduler
}

func New(store execution.Store, options ...Option) (*App, error) {
	if store == nil {
		return nil, errors.New("ody store is nil")
	}
	config := configuration{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	tasks, err := task.NewRegistry(config.modules...)
	if err != nil {
		return nil, fmt.Errorf("build task registry: %w", err)
	}
	pipelines, err := pipeline.NewRegistry(config.pipelines...)
	if err != nil {
		return nil, fmt.Errorf("build pipeline registry: %w", err)
	}
	engine, err := pipeline.NewEngine(store, pipelines)
	if err != nil {
		return nil, err
	}
	runner, err := worker.New(store, tasks, engine, config.observer, config.worker)
	if err != nil {
		return nil, err
	}
	scheduler, err := schedule.New(store, engine, config.schedules, config.scheduler)
	if err != nil {
		return nil, err
	}
	return &App{store: store, worker: runner, pipelines: engine, scheduler: scheduler}, nil
}

func (a *App) Run(ctx context.Context) error {
	if a == nil || a.worker == nil {
		return errors.New("ody app is nil")
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var group sync.WaitGroup
	for _, component := range []interface{ Run(context.Context) error }{a.worker, a.scheduler} {
		group.Go(func() {
			if err := component.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				cancel(err)
			}
		})
	}
	group.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return context.Cause(runCtx)
}

func (a *App) CreateExecution(ctx context.Context, request execution.CreateExecution) (execution.Execution, error) {
	if a == nil || a.store == nil {
		return execution.Execution{}, errors.New("ody app is nil")
	}
	return a.store.CreateExecution(ctx, request)
}

func (a *App) RestartExecution(ctx context.Context, id uuid.UUID) (execution.Execution, error) {
	if a == nil || a.pipelines == nil {
		return execution.Execution{}, errors.New("ody app is nil")
	}
	return a.pipelines.RestartExecution(ctx, id)
}

func (a *App) PipelineEngine() *pipeline.Engine { return a.pipelines }
func (a *App) Worker() *worker.Worker           { return a.worker }
func (a *App) Store() execution.Store           { return a.store }
