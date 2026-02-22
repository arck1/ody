package schedulor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	queue2 "schedulor/queue"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

// LqExecutor polls queue backend and dispatches claimed tasks to selected executor strategy.
type LqExecutor struct {
	// Id is a unique executor identity used for diagnostics.
	Id string
	// db provides SQL connections.
	db DbConnector
	// logger writes executor events and errors.
	logger *zap.SugaredLogger
	// queue is a backend used to claim/ack/nack tasks.
	queue queue2.QueueBackend
	// exec performs actual task business logic.
	exec TaskExecutor
	// options controls polling and batching behavior.
	options LqExecutorOptions
}

// NewLqExecutor builds executor with default queue backend and selected execution mode.
func NewLqExecutor(
	db DbConnector,
	logger *zap.SugaredLogger,
	tasks []TaskHandler,
	options *LqExecutorOptions,
) *LqExecutor {
	settings := GetSettings(&LqSettings{
		LqExecutorOptions: options,
	}).LqExecutorOptions
	var taskExecutor TaskExecutor = NewCodeTaskExecutor(tasks)
	switch settings.ExecutorMode {
	case "", "code":
	case "bash_file":
		var err error
		taskExecutor, err = NewBashFileTaskExecutorFromFile(settings.BashCommandsFile)
		if err != nil {
			panic(fmt.Errorf("init bash_file executor: %w", err))
		}
	default:
		panic(fmt.Errorf("unknown executor mode %q", settings.ExecutorMode))
	}
	queueOptions := queue2.PostgresQueueOptions{
		TaskMaxAttempts: defaultSettings.TaskMaxAttempts,
		TaskVisibility:  defaultSettings.TaskVisibility,
	}
	return NewLqExecutorWith(
		db,
		logger,
		queue2.NewPostgresQueue(db, &queueOptions),
		taskExecutor,
		settings,
	)
}

// NewLqExecutorWith builds executor with explicitly provided queue backend and executor strategy.
func NewLqExecutorWith(
	db DbConnector,
	logger *zap.SugaredLogger,
	backend queue2.QueueBackend,
	exec TaskExecutor,
	options *LqExecutorOptions,
) *LqExecutor {
	options = GetSettings(&LqSettings{
		LqExecutorOptions: options,
	}).LqExecutorOptions
	if exec == nil {
		exec = NewCodeTaskExecutor(nil)
	}
	if backend == nil {
		queueOptions := queue2.PostgresQueueOptions{
			TaskMaxAttempts: defaultSettings.TaskMaxAttempts,
			TaskVisibility:  defaultSettings.TaskVisibility,
		}
		backend = queue2.NewPostgresQueue(db, &queueOptions)
	}
	return &LqExecutor{
		Id:      getLeaderId(true),
		db:      db,
		logger:  logger,
		options: *options,
		queue:   backend,
		exec:    exec,
	}
}

// GetQueue returns queue producer interface used by scheduler to enqueue tasks.
func (e *LqExecutor) GetQueue() queue2.TasksQueue {
	return e.queue
}

// GetOptions returns resolved executor options.
func (e *LqExecutor) GetOptions() LqExecutorOptions { return e.options }

// Init registers executor lifecycle hooks in fx.
func (e *LqExecutor) Init(lifecycle fx.Lifecycle) {
	executorCtx, cancel := context.WithCancel(context.Background())
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go e.Run(executorCtx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			return nil
		},
	})
}

// Run starts polling loop that claims tasks and processes them.
func (e *LqExecutor) Run(ctx context.Context) {
	tasksNames := e.exec.TaskNames()
	if len(tasksNames) == 0 {
		e.logger.Warn("executor has no task names to process")
		return
	}
	var unknownTaskError *UnknownTaskName

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		claimed, err := e.queue.Claim(ctx, tasksNames, e.options.PoolingBatch)
		if err != nil {
			e.logger.Warnw("claim error", "err", err)
			time.Sleep(e.options.PoolingTimeout)
			continue
		}
		if len(claimed) == 0 {
			time.Sleep(e.options.PoolingTimeout)
			continue
		}

		for _, task := range claimed {
			select {
			case <-ctx.Done():
				return
			default:
			}
			jobCtx, cancel := context.WithCancel(ctx)
			lost := make(chan struct{}, 1)
			go e.queue.StartHeartbeat(jobCtx, task.TaskID, task.LeaseToken, lost)

			// Работа
			err = e.processTask(jobCtx, task)
			cancel() // остановить heartbeat

			select {
			case <-lost:
				// аренду потеряли — не ack’аем; задачу подберут другие
				continue
			default:
			}

			if errors.As(err, &unknownTaskError) {
				_, _ = e.queue.Nack(ctx, task.TaskID, task.LeaseToken, err.Error(), 0)
			} else if err != nil {
				backoff := ExponentialBackoff(task.Attempts, task.MaxAttempts)
				_, _ = e.queue.Nack(ctx, task.TaskID, task.LeaseToken, err.Error(), backoff)

				if task.Attempts >= task.MaxAttempts {
					_, _ = e.queue.MoveToDLQ(ctx, task.TaskID)
				}
			} else {
				_, _ = e.queue.Ack(ctx, task.TaskID, task.LeaseToken)
			}
		}
	}
}

// processTask executes a single task and converts panics to errors.
func (e *LqExecutor) processTask(ctx context.Context, task queue2.Claimed) (err error) {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			e.logger.Errorw(
				"task handler panic",
				"task_id", task.TaskID,
				"task_name", task.TaskName,
				"panic", panicErr,
				"stack", string(debug.Stack()),
			)
			err = fmt.Errorf("task handler panic: %v", panicErr)
		}
	}()
	return e.exec.Execute(ctx, task)
}
