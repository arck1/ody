package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"schedulor/queue"

	"github.com/samber/lo"
)

// BashFileTaskExecutor executes shell commands mapped by task name from config file.
type BashFileTaskExecutor struct {
	// commands maps task names to shell commands.
	commands map[string]string
}

// NewBashFileTaskExecutor creates executor from provided task->command mapping.
func NewBashFileTaskExecutor(commands map[string]string) *BashFileTaskExecutor {
	return &BashFileTaskExecutor{commands: commands}
}

// NewBashFileTaskExecutorFromFile loads executor command mapping from JSON file.
func NewBashFileTaskExecutorFromFile(path string) (*BashFileTaskExecutor, error) {
	commands, err := loadBashCommands(path)
	if err != nil {
		return nil, err
	}
	return NewBashFileTaskExecutor(commands), nil
}

// TaskNames returns task names available in loaded command mapping.
func (e *BashFileTaskExecutor) TaskNames() []string { return lo.Keys(e.commands) }

// Execute runs preconfigured bash command for claimed task name.
func (e *BashFileTaskExecutor) Execute(ctx context.Context, task queue.Claimed) error {
	command, ok := e.commands[task.TaskName]
	if !ok {
		return &UnknownTaskName{TaskId: task.TaskID, TaskName: task.TaskName}
	}
	cmd := exec.CommandContext(ctx, "bash", "-lc", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("bash command failed for task %q: %w, output=%s", task.TaskName, err, string(output))
	}
	return nil
}

type bashCommandsConfig struct {
	// Tasks is compact map representation task_name -> command.
	Tasks map[string]string `json:"tasks"`
	// List is verbose list representation of commands.
	List []struct {
		TaskName string `json:"task_name"`
		Command  string `json:"command"`
	} `json:"list"`
}

// loadBashCommands parses JSON config and returns normalized command map.
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
