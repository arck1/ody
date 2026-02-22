package executor

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

func runCommand(ctx context.Context, args []string) ([]byte, error) {
	if len(args) == 0 || args[0] == "" {
		return nil, fmt.Errorf("command args are empty")
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	return cmd.CombinedOutput()
}

func parseCommandString(command string) ([]string, error) {
	args := strings.Fields(command)
	if len(args) == 0 {
		return nil, fmt.Errorf("command must be non-empty")
	}
	return args, nil
}

func parseCommandValue(raw any, fieldName string) ([]string, error) {
	switch value := raw.(type) {
	case string:
		args, err := parseCommandString(value)
		if err != nil {
			return nil, fmt.Errorf("payload field %q must be non-empty command", fieldName)
		}
		return args, nil
	case []string:
		if len(value) == 0 {
			return nil, fmt.Errorf("payload field %q command args must be non-empty", fieldName)
		}
		return value, nil
	case []any:
		if len(value) == 0 {
			return nil, fmt.Errorf("payload field %q command args must be non-empty", fieldName)
		}
		args := make([]string, 0, len(value))
		for _, item := range value {
			text, ok := item.(string)
			if !ok || text == "" {
				return nil, fmt.Errorf("payload field %q command args must contain only non-empty strings", fieldName)
			}
			args = append(args, text)
		}
		return args, nil
	default:
		return nil, fmt.Errorf("payload field %q must be string or array of strings", fieldName)
	}
}
