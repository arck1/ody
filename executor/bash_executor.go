package executor

import (
	"context"
	"fmt"
	"schedulor/queue"

	"github.com/samber/lo"
)

// BashTaskExecutor executes configured commands by task name or from payload.
type BashTaskExecutor struct {
	// taskNames limits which task names are polled when command comes from payload.
	taskNames []string
	// commandField points to payload field containing command line to execute.
	commandField string
	// commands maps task names to direct command execution entries.
	commands map[string]TaskCommand
}

// TaskCommand describes direct command execution settings for one task.
type TaskCommand struct {
	// Command is a single command line string split into binary + args.
	Command string
	// Args is explicit argv representation; it has priority over Command.
	Args []string
}

// NewBashTaskExecutor creates executor that reads command from payload.
func NewBashTaskExecutor(taskNames []string, commandField string) *BashTaskExecutor {
	if len(taskNames) == 0 {
		taskNames = []string{"bash"}
	}
	if commandField == "" {
		commandField = "command"
	}
	return &BashTaskExecutor{taskNames: taskNames, commandField: commandField}
}

// NewBashTaskExecutorFromCommands creates executor from provided task->command mapping.
func NewBashTaskExecutorFromCommands(commands map[string]string) *BashTaskExecutor {
	entries := make(map[string]TaskCommand, len(commands))
	for taskName, command := range commands {
		entries[taskName] = TaskCommand{Command: command}
	}
	return &BashTaskExecutor{commands: entries}
}

// NewBashTaskExecutorWithCommands creates executor from extended task command config.
func NewBashTaskExecutorWithCommands(commands map[string]TaskCommand) *BashTaskExecutor {
	return &BashTaskExecutor{commands: commands}
}

// TaskNames returns supported task names.
func (e *BashTaskExecutor) TaskNames() []string {
	if len(e.commands) > 0 {
		return lo.Keys(e.commands)
	}
	return e.taskNames
}

// Execute runs configured task command or payload command.
func (e *BashTaskExecutor) Execute(ctx context.Context, task queue.Claimed) error {
	if len(e.commands) == 0 {
		return e.executeFromPayload(ctx, task)
	}
	command, ok := e.commands[task.TaskName]
	if !ok {
		return &UnknownTaskName{TaskId: task.TaskID, TaskName: task.TaskName}
	}
	args := command.Args
	var err error
	if len(args) == 0 {
		args, err = parseCommandString(command.Command)
		if err != nil {
			return fmt.Errorf("invalid configured command for task %q", task.TaskName)
		}
	}
	output, err := runCommand(ctx, args)
	if err != nil {
		return fmt.Errorf("command failed for task %q: %w, output=%s", task.TaskName, err, string(output))
	}
	return nil
}

func (e *BashTaskExecutor) executeFromPayload(ctx context.Context, task queue.Claimed) error {
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
