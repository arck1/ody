package schedulor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadBashTaskCommandsFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := filepath.Join(tmpDir, "commands.json")
	content := `{
		"tasks": {"a":"echo a"},
		"list": [
			{"task_name":"b","command":"echo b"},
			{"task_name":"c","args":["echo","c"]}
		]
	}`
	if err := os.WriteFile(cfg, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	commands, err := LoadBashTaskCommandsFromFile(cfg)
	if err != nil {
		t.Fatalf("load commands: %v", err)
	}
	if len(commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(commands))
	}
	if commands["a"].Command != "echo a" {
		t.Fatalf("unexpected command for a: %+v", commands["a"])
	}
	if len(commands["c"].Args) != 2 || commands["c"].Args[0] != "echo" {
		t.Fatalf("unexpected args for c: %+v", commands["c"].Args)
	}
}

func TestLoadBashTaskCommandsFromFileValidation(t *testing.T) {
	_, err := LoadBashTaskCommandsFromFile("")
	if err == nil || !strings.Contains(err.Error(), "path is empty") {
		t.Fatalf("unexpected empty path error: %v", err)
	}

	tmpDir := t.TempDir()
	badJSON := filepath.Join(tmpDir, "bad.json")
	if err = os.WriteFile(badJSON, []byte(`{`), 0o600); err != nil {
		t.Fatalf("write bad json: %v", err)
	}
	_, err = LoadBashTaskCommandsFromFile(badJSON)
	if err == nil || !strings.Contains(err.Error(), "parse bash commands file") {
		t.Fatalf("unexpected parse error: %v", err)
	}

	emptyCfg := filepath.Join(tmpDir, "empty.json")
	if err = os.WriteFile(emptyCfg, []byte(`{"tasks":{}}`), 0o600); err != nil {
		t.Fatalf("write empty config: %v", err)
	}
	_, err = LoadBashTaskCommandsFromFile(emptyCfg)
	if err == nil || !strings.Contains(err.Error(), "contains no runnable tasks") {
		t.Fatalf("unexpected empty config error: %v", err)
	}
}
