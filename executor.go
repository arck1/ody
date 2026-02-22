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

type LqExecutor struct {
	Id      string
	db      DbConnector
	logger  *zap.SugaredLogger
	queue   queue2.QueueBackend
	exec    TaskExecutor
	options LqExecutorOptions
}

func NewLqExecutor(
	db DbConnector,
	logger *zap.SugaredLogger,
	tasks []TaskHandler,
	options *LqExecutorOptions,
) *LqExecutor {
	settings := GetSettings(&LqSettings{
		LqExecutorOptions: options,
	}).LqExecutorOptions
	queueOptions := queue2.PostgresQueueOptions{
		TaskMaxAttempts: defaultSettings.TaskMaxAttempts,
		TaskVisibility:  defaultSettings.TaskVisibility,
	}
	return NewLqExecutorWith(
		db,
		logger,
		queue2.NewPostgresQueue(db, &queueOptions),
		NewCodeTaskExecutor(tasks),
		settings,
	)
}

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

func (e *LqExecutor) GetQueue() queue2.TasksQueue {
	return e.queue
}
func (e *LqExecutor) GetOptions() LqExecutorOptions { return e.options }

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
