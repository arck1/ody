// Package zapobserver adapts structured logging to worker.Observer without coupling worker core to
// a concrete logger.
package zapobserver

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"ody/execution"
	"ody/worker"
)

// Logger is intentionally compatible with zap.SugaredLogger and simple application loggers.
type Logger interface {
	Infow(string, ...any)
	Errorw(string, ...any)
}

type Observer struct{ logger Logger }

func New(logger Logger) (*Observer, error) {
	if logger == nil {
		return nil, errors.New("worker logger is nil")
	}
	return &Observer{logger: logger}, nil
}

// NewSugared is the ready-to-use adapter for zap while New remains logger-implementation neutral.
func NewSugared(logger *zap.SugaredLogger) (*Observer, error) { return New(logger) }

func (o *Observer) Transition(_ context.Context, item execution.Execution, status execution.Status, transitionErr error) {
	fields := []any{
		"execution_id", item.ID,
		"task", item.TaskName,
		"task_version", item.TaskVersion,
		"attempt", item.Attempt,
		"status", status,
	}
	if transitionErr != nil {
		fields = append(fields, "error", transitionErr)
		o.logger.Errorw("task transition", fields...)
		return
	}
	o.logger.Infow("task transition", fields...)
}

func (o *Observer) InfrastructureError(_ context.Context, operation worker.Operation, err error) {
	o.logger.Errorw("worker infrastructure error", "operation", operation, "error", err)
}

var _ worker.Observer = (*Observer)(nil)
