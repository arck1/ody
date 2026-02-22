package schedulor

import (
	"encoding/json"
	"fmt"
	"os"
)

type bashTaskCommandsConfig struct {
	// Tasks is compact map representation task_name -> command.
	Tasks map[string]string `json:"tasks"`
	// List is verbose list representation of commands.
	List []struct {
		TaskName string   `json:"task_name"`
		Command  string   `json:"command"`
		Args     []string `json:"args"`
	} `json:"list"`
}

// LoadBashTaskCommandsFromFile parses JSON config and returns normalized command map.
func LoadBashTaskCommandsFromFile(path string) (map[string]BashTaskCommand, error) {
	if path == "" {
		return nil, fmt.Errorf("bash commands file path is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read bash commands file: %w", err)
	}
	var cfg bashTaskCommandsConfig
	if err = json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse bash commands file: %w", err)
	}
	commands := make(map[string]BashTaskCommand, len(cfg.Tasks)+len(cfg.List))
	for taskName, command := range cfg.Tasks {
		if taskName == "" || command == "" {
			continue
		}
		commands[taskName] = BashTaskCommand{Command: command}
	}
	for _, item := range cfg.List {
		if item.TaskName == "" {
			continue
		}
		if len(item.Args) > 0 {
			commands[item.TaskName] = BashTaskCommand{Args: item.Args}
			continue
		}
		if item.Command != "" {
			commands[item.TaskName] = BashTaskCommand{Command: item.Command}
		}
	}
	if len(commands) == 0 {
		return nil, fmt.Errorf("bash commands file contains no runnable tasks")
	}
	return commands, nil
}
