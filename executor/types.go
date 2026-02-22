package executor

import (
	"context"
	"fmt"
	queue2 "schedulor/queue"
)

type TaskHandlerFunc func(ctx context.Context, payload map[string]interface{}) error

type TaskHandler struct {
	TaskName string
	Handler  TaskHandlerFunc
}

type TaskExecutor interface {
	TaskNames() []string
	Execute(ctx context.Context, task queue2.Claimed) error
}

type UnknownTaskName struct {
	TaskId   int64
	TaskName string
}

func (e *UnknownTaskName) Error() string {
	return fmt.Sprintf("unknown task name %s: %d", e.TaskName, e.TaskId)
}
