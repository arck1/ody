package executor

import (
	"context"
	"fmt"
	"os/exec"
	queue2 "schedulor/queue"
)

type BashPayloadTaskExecutor struct {
	taskNames    []string
	commandField string
}

func NewBashPayloadTaskExecutor(taskNames []string, commandField string) *BashPayloadTaskExecutor {
	if len(taskNames) == 0 {
		taskNames = []string{"bash"}
	}
	if commandField == "" {
		commandField = "command"
	}
	return &BashPayloadTaskExecutor{taskNames: taskNames, commandField: commandField}
}

func (e *BashPayloadTaskExecutor) TaskNames() []string { return e.taskNames }

func (e *BashPayloadTaskExecutor) Execute(ctx context.Context, task queue2.Claimed) error {
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
