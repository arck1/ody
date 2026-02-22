package schedulor

import (
	"context"
)

type TaskHandlerFunc func(ctx context.Context, payload map[string]interface{}) error

type TaskHandler struct {
	TaskName string
	Handler  TaskHandlerFunc
}
