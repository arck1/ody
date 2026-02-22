package schedulor

import (
	"context"
	"fmt"
	"os/exec"
	queue2 "schedulor/queue"

	"github.com/samber/lo"
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

type BashTaskExecutor struct {
	taskNames    []string
	commandField string
}

func NewBashTaskExecutor(taskNames []string, commandField string) *BashTaskExecutor {
	if len(taskNames) == 0 {
		taskNames = []string{"bash"}
	}
	if commandField == "" {
		commandField = "command"
	}
	return &BashTaskExecutor{
		taskNames:    taskNames,
		commandField: commandField,
	}
}

func (e *BashTaskExecutor) TaskNames() []string {
	return e.taskNames
}

func (e *BashTaskExecutor) Execute(ctx context.Context, task queue2.Claimed) error {
	commandRaw, ok := task.Payload.Data()[e.commandField]
	if !ok {
		return fmt.Errorf("payload field %q is required for bash executor", e.commandField)
	}
	command, ok := commandRaw.(string)
	if !ok || command == "" {
		return fmt.Errorf("payload field %q must be non-empty string", e.commandField)
	}

	cmd := exec.CommandContext(ctx, "bash", "-lc", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("bash command failed: %w, output=%s", err, string(output))
	}
	return nil
}
