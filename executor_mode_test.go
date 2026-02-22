package schedulor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewLqExecutorBashFileMode(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "commands.json")
	if err := os.WriteFile(configPath, []byte(`{"tasks":{"bash_task":"echo ok"}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	exec := NewLqExecutor(nil, testLogger(), nil, &LqExecutorOptions{
		PoolingTimeout:   10,
		PoolingBatch:     1,
		ExecutorMode:     "bash_file",
		BashCommandsFile: configPath,
	})
	if _, ok := exec.exec.(*BashFileTaskExecutor); !ok {
		t.Fatalf("expected BashFileTaskExecutor, got %T", exec.exec)
	}
}

func TestNewLqExecutorUnknownModePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic for unknown mode")
		}
	}()
	_ = NewLqExecutor(nil, testLogger(), nil, &LqExecutorOptions{
		PoolingTimeout: 10,
		PoolingBatch:   1,
		ExecutorMode:   "unknown",
	})
}
