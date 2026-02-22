package schedulor

import (
	"schedulor/queue"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

// FxAppOptions configures application assembly for running schedulor via fx.
type FxAppOptions struct {
	// DB is SQL connector used by queue, elector and scheduler.
	DB DbConnector
	// Logger is shared application logger.
	Logger *zap.SugaredLogger
	// QueueBackend is runtime queue backend implementation.
	QueueBackend queue.QueueBackend
	// TaskExecutor is runtime strategy for task execution.
	TaskExecutor TaskExecutor
	// ExecutorOptions configures polling and batching.
	ExecutorOptions *LqExecutorOptions
	// SchedulerOptions configures schedule refresh and leader behavior.
	SchedulerOptions *LqSchedulerOptions
}

// NewFxApp creates ready-to-run fx app with LqExecutor and LqScheduler wired in.
func NewFxApp(options FxAppOptions) *fx.App {
	validateFxOptions(options)

	return fx.New(
		fx.Provide(
			func() DbConnector { return options.DB },
			func() *zap.SugaredLogger { return options.Logger },
			func() queue.QueueBackend { return options.QueueBackend },
			func() TaskExecutor { return options.TaskExecutor },
			func() *LqExecutorOptions { return options.ExecutorOptions },
			func() *LqSchedulerOptions { return options.SchedulerOptions },
		),
		fx.Provide(NewLqExecutor),
		fx.Provide(NewLqScheduler),
		fx.Invoke(registerExecutorLifecycle),
		fx.Invoke(registerSchedulerLifecycle),
	)
}

func validateFxOptions(options FxAppOptions) {
	if options.DB == nil {
		panic("fx app option DB is nil")
	}
	if options.Logger == nil {
		panic("fx app option Logger is nil")
	}
	if options.QueueBackend == nil {
		panic("fx app option QueueBackend is nil")
	}
	if options.TaskExecutor == nil {
		panic("fx app option TaskExecutor is nil")
	}
}

func registerExecutorLifecycle(lifecycle fx.Lifecycle, executor *LqExecutor) {
	executor.Init(lifecycle)
}

func registerSchedulerLifecycle(lifecycle fx.Lifecycle, scheduler *LqScheduler) {
	scheduler.Init(lifecycle)
}
