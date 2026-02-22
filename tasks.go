package schedulor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

type BashFileTaskExecutor struct {
	commands map[string]string
}

func NewBashFileTaskExecutor(commands map[string]string) *BashFileTaskExecutor {
	return &BashFileTaskExecutor{commands: commands}
}

func NewBashFileTaskExecutorFromFile(path string) (*BashFileTaskExecutor, error) {
	commands, err := loadBashCommands(path)
	if err != nil {
		return nil, err
	}
	return NewBashFileTaskExecutor(commands), nil
}

func (e *BashFileTaskExecutor) TaskNames() []string {
	return lo.Keys(e.commands)
}

func (e *BashFileTaskExecutor) Execute(ctx context.Context, task queue2.Claimed) error {
	command, ok := e.commands[task.TaskName]
	if !ok {
		return &UnknownTaskName{
			TaskId:   task.TaskID,
			TaskName: task.TaskName,
		}
	}
	cmd := exec.CommandContext(ctx, "bash", "-lc", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("bash command failed for task %q: %w, output=%s", task.TaskName, err, string(output))
	}
	return nil
}

type bashCommandsConfig struct {
	Tasks map[string]string `json:"tasks"`
	List  []struct {
		TaskName string `json:"task_name"`
		Command  string `json:"command"`
	} `json:"list"`
}

func loadBashCommands(path string) (map[string]string, error) {
	if path == "" {
		return nil, fmt.Errorf("bash commands file path is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read bash commands file: %w", err)
	}
	var cfg bashCommandsConfig
	if err = json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse bash commands file: %w", err)
	}
	commands := make(map[string]string, len(cfg.Tasks)+len(cfg.List))
	for taskName, command := range cfg.Tasks {
		if taskName == "" || command == "" {
			continue
		}
		commands[taskName] = command
	}
	for _, item := range cfg.List {
		if item.TaskName == "" || item.Command == "" {
			continue
		}
		commands[item.TaskName] = item.Command
	}
	if len(commands) == 0 {
		return nil, fmt.Errorf("bash commands file contains no runnable tasks")
	}
	return commands, nil
}
