package executor

import (
	"context"
	"fmt"

	"schedulor/queue"
)

// TaskHandlerFunc processes task payload decoded from queue JSON.
type TaskHandlerFunc func(ctx context.Context, payload map[string]any) error

// TaskHandler binds queue task name to handler function.
type TaskHandler struct {
	// TaskName is a logical name referenced by queued task rows.
	TaskName string
	// Handler contains business logic implementation.
	Handler TaskHandlerFunc
}

// TaskExecutor defines pluggable task execution strategy.
type TaskExecutor interface {
	// TaskNames returns list of queue task names processed by executor.
	TaskNames() []string
	// Execute processes a single claimed task.
	Execute(ctx context.Context, task queue.Claimed) error
}

// UnknownTaskName is returned when task name has no registered handler/command.
type UnknownTaskName struct {
	// TaskId is runtime task id.
	TaskId int64
	// TaskName is unregistered task name.
	TaskName string
}

// Error implements error interface for unknown task type.
func (e *UnknownTaskName) Error() string {
	return fmt.Sprintf("unknown task name %s: %d", e.TaskName, e.TaskId)
}
