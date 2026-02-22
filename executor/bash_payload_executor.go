package executor

import (
	"context"
	"fmt"
	"schedulor/queue"
)

// BashPayloadTaskExecutor executes shell commands from task payload fields.
type BashPayloadTaskExecutor struct {
	// taskNames limits which queue task names are polled.
	taskNames []string
	// commandField points to payload field containing shell command.
	commandField string
}

// NewBashPayloadTaskExecutor creates executor that reads command from task payload.
func NewBashPayloadTaskExecutor(taskNames []string, commandField string) *BashPayloadTaskExecutor {
	if len(taskNames) == 0 {
		taskNames = []string{"bash"}
	}
	if commandField == "" {
		commandField = "command"
	}
	return &BashPayloadTaskExecutor{taskNames: taskNames, commandField: commandField}
}

// TaskNames returns supported task names.
func (e *BashPayloadTaskExecutor) TaskNames() []string { return e.taskNames }

// Execute runs bash command from payload field.
func (e *BashPayloadTaskExecutor) Execute(ctx context.Context, task queue.Claimed) error {
	commandRaw, ok := task.Payload.Data()[e.commandField]
	if !ok {
		return fmt.Errorf("payload field %q is required for bash executor", e.commandField)
	}
	args, err := parseCommandValue(commandRaw, e.commandField)
	if err != nil {
		return err
	}
	output, err := runCommand(ctx, args)
	if err != nil {
		return fmt.Errorf("command failed: %w, output=%s", err, string(output))
	}
	return nil
}
