package schedulor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/samber/lo"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

type LqExecutor struct {
	Id      string
	db      DbConnector
	logger  *zap.SugaredLogger
	queue   *PostgresQueue
	tasks   map[string]TaskHandlerFunc
	options LqExecutorOptions
}

func NewLqExecutor(
	db DbConnector,
	logger *zap.SugaredLogger,
	tasks []TaskHandler,
	options *LqExecutorOptions,
) *LqExecutor {
	options = GetSettings(&LqSettings{
		LqExecutorOptions: options,
	}).LqExecutorOptions
	return &LqExecutor{
		Id:      getLeaderId(true),
		db:      db,
		logger:  logger,
		options: *options,
		queue:   NewPostgresQueue(db, nil),
		tasks: lo.Associate(tasks, func(item TaskHandler) (string, TaskHandlerFunc) {
			return item.TaskName, item.Handler
		}),
	}
}

func (e *LqExecutor) GetQueue() TasksQueue {
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
	tasksNames := lo.Keys(e.tasks)
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

func (e *LqExecutor) processTask(ctx context.Context, task Claimed) (err error) {
	if handler, ok := e.tasks[task.TaskName]; !ok {
		return &UnknownTaskName{
			TaskId:   task.TaskID,
			TaskName: task.TaskName,
		}
	} else {
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

		return handler(ctx, task.Payload.Data())
	}
}
