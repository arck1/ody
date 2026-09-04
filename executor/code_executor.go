package executor

import (
	"context"

	"schedulor/queue"

	"github.com/samber/lo"
)

// CodeTaskExecutor executes tasks using in-process Go handlers.
type CodeTaskExecutor struct {
	// tasks is map of task name to business handler.
	tasks map[string]TaskHandlerFunc
}

// NewCodeTaskExecutor creates in-process executor from handlers list.
func NewCodeTaskExecutor(tasks []TaskHandler) *CodeTaskExecutor {
	return &CodeTaskExecutor{
		tasks: lo.Associate(tasks, func(item TaskHandler) (string, TaskHandlerFunc) {
			return item.TaskName, item.Handler
		}),
	}
}

// TaskNames returns registered task names.
func (e *CodeTaskExecutor) TaskNames() []string {
	return lo.Keys(e.tasks)
}

// Execute runs mapped Go handler for claimed task.
func (e *CodeTaskExecutor) Execute(ctx context.Context, task queue.Claimed) error {
	handler, ok := e.tasks[task.TaskName]
	if !ok {
		return &UnknownTaskName{
			TaskId:   task.TaskID,
			TaskName: task.TaskName,
		}
	}
	return handler(ctx, task.Payload.Data())
}
