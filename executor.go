package schedulor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"schedulor/queue"

	"go.uber.org/fx"
)

// LqExecutor polls queue backend and dispatches claimed tasks to selected executor strategy.
type LqExecutor struct {
	// Id is a unique executor identity used for diagnostics.
	Id string
	// logger writes executor events and errors.
	logger Logger
	// queue is a backend used to claim/ack/nack tasks.
	queue queue.Backend
	// exec performs actual task business logic.
	exec TaskExecutor
	// options controls polling and batching behavior.
	options LqExecutorOptions
	// lifecycleWG tracks the polling loop started by the fx lifecycle.
	lifecycleWG sync.WaitGroup
}

// NewLqExecutor builds executor from explicit interface dependencies.
func NewLqExecutor(
	logger Logger,
	backend queue.Backend,
	exec TaskExecutor,
	options *LqExecutorOptions,
) (*LqExecutor, error) {
	settings := GetSettings(&LqSettings{
		LqExecutorOptions: options,
	}).LqExecutorOptions
	if logger == nil {
		return nil, fmt.Errorf("executor logger is nil")
	}
	if backend == nil {
		return nil, fmt.Errorf("executor queue backend is nil")
	}
	if exec == nil {
		return nil, fmt.Errorf("task executor strategy is nil")
	}
	return &LqExecutor{
		Id:      getLeaderId(true),
		logger:  logger,
		options: *settings,
		queue:   backend,
		exec:    exec,
	}, nil
}

// GetQueue returns queue producer interface used by scheduler to enqueue tasks.
func (e *LqExecutor) GetQueue() queue.TasksQueue {
	return e.queue
}

// GetOptions returns resolved executor options.
func (e *LqExecutor) GetOptions() LqExecutorOptions { return e.options }

// Init registers executor lifecycle hooks in fx.
func (e *LqExecutor) Init(lifecycle fx.Lifecycle) {
	executorCtx, cancel := context.WithCancel(context.Background())
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			e.lifecycleWG.Go(func() {
				e.Run(executorCtx)
			})
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			stopped := make(chan struct{})
			go func() {
				e.lifecycleWG.Wait()
				close(stopped)
			}()
			select {
			case <-stopped:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("stop executor: %w", ctx.Err())
			}
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

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		claimed, err := e.queue.Claim(ctx, tasksNames, e.options.PoolingBatch)
		if err != nil {
			e.logger.Warn("claim error", "err", err)
			if !waitForPoll(ctx, e.options.PoolingTimeout) {
				return
			}
			continue
		}
		if len(claimed) == 0 {
			if !waitForPoll(ctx, e.options.PoolingTimeout) {
				return
			}
			continue
		}

		type activeTask struct {
			task   queue.Claimed
			ctx    context.Context
			cancel context.CancelFunc
			lost   chan struct{}
		}
		active := make([]activeTask, 0, len(claimed))
		for _, task := range claimed {
			jobCtx, cancel := context.WithCancel(ctx)
			lost := make(chan struct{}, 1)
			active = append(active, activeTask{task: task, ctx: jobCtx, cancel: cancel, lost: lost})
			go e.queue.StartHeartbeat(jobCtx, task.TaskID, task.LeaseToken, lost)
		}

		for i, item := range active {
			select {
			case <-ctx.Done():
				for _, pending := range active[i:] {
					pending.cancel()
				}
				return
			default:
			}

			// The lease heartbeat is already running for every task in the claimed batch.
			err = e.processTask(item.ctx, item.task)
			if ctx.Err() != nil {
				for _, pending := range active[i:] {
					pending.cancel()
				}
				return
			}

			select {
			case <-item.lost:
				item.cancel()
				e.logger.Warn("task lease lost", "task_id", item.task.TaskID, "task_name", item.task.TaskName)
				continue
			default:
			}
			item.cancel()

			if _, ok := errors.AsType[*UnknownTaskName](err); ok {
				e.moveToDLQ(ctx, item.task, err)
			} else if err != nil {
				if item.task.Attempts >= item.task.MaxAttempts {
					e.moveToDLQ(ctx, item.task, err)
				} else {
					backoff := ExponentialBackoff(item.task.Attempts, item.task.MaxAttempts)
					ok, nackErr := e.queue.Nack(ctx, item.task.TaskID, item.task.LeaseToken, err.Error(), backoff)
					if nackErr != nil || !ok {
						e.logger.Error("failed to nack task", "task_id", item.task.TaskID, "task_name", item.task.TaskName, "owned", ok, "err", nackErr)
					}
				}
			} else {
				ok, ackErr := e.queue.Ack(ctx, item.task.TaskID, item.task.LeaseToken)
				if ackErr != nil || !ok {
					e.logger.Error("failed to ack task", "task_id", item.task.TaskID, "task_name", item.task.TaskName, "owned", ok, "err", ackErr)
				}
			}
		}
	}
}

func (e *LqExecutor) moveToDLQ(ctx context.Context, task queue.Claimed, taskErr error) {
	ok, err := e.queue.MoveToDLQ(ctx, task.TaskID, task.LeaseToken, taskErr.Error())
	if err != nil || !ok {
		e.logger.Error(
			"failed to move task to dlq",
			"task_id", task.TaskID,
			"task_name", task.TaskName,
			"owned", ok,
			"err", err,
		)
	}
}

func waitForPoll(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// processTask executes a single task and converts panics to errors.
func (e *LqExecutor) processTask(ctx context.Context, task queue.Claimed) (err error) {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			e.logger.Error(
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
