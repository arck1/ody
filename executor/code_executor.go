package executor

import (
	"context"
	queue2 "schedulor/queue"

	"github.com/samber/lo"
)

type CodeTaskExecutor struct {
	tasks map[string]TaskHandlerFunc
}

func NewCodeTaskExecutor(tasks []TaskHandler) *CodeTaskExecutor {
	return &CodeTaskExecutor{
		tasks: lo.Associate(tasks, func(item TaskHandler) (string, TaskHandlerFunc) {
			return item.TaskName, item.Handler
		}),
	}
}

func (e *CodeTaskExecutor) TaskNames() []string {
	return lo.Keys(e.tasks)
}

func (e *CodeTaskExecutor) Execute(ctx context.Context, task queue2.Claimed) error {
	handler, ok := e.tasks[task.TaskName]
	if !ok {
		return &UnknownTaskName{
			TaskId:   task.TaskID,
			TaskName: task.TaskName,
		}
	}
	return handler(ctx, task.Payload.Data())
}
