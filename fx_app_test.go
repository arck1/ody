package schedulor

import (
	"context"
	"database/sql"
	"testing"
)

type testDBConnector struct{}

func (t testDBConnector) GetConnect(ctx context.Context) (*sql.DB, error) {
	return nil, nil
}

func TestNewFxAppBuilds(t *testing.T) {
	app := NewFxApp(FxAppOptions{
		DB:           testDBConnector{},
		Logger:       testLogger(),
		QueueBackend: &testQueueBackend{},
		TaskExecutor: NewCodeTaskExecutor(nil),
		ExecutorOptions: &LqExecutorOptions{
			PoolingTimeout: 1,
			PoolingBatch:   1,
		},
		SchedulerOptions: &LqSchedulerOptions{
			TasksRefreshEnabled: false,
		},
	})
	if app == nil {
		t.Fatalf("expected app instance")
	}
}

func TestNewFxAppPanicsOnMissingDeps(t *testing.T) {
	tests := []struct {
		name string
		opts FxAppOptions
	}{
		{
			name: "missing db",
			opts: FxAppOptions{
				Logger:       testLogger(),
				QueueBackend: &testQueueBackend{},
				TaskExecutor: NewCodeTaskExecutor(nil),
			},
		},
		{
			name: "missing logger",
			opts: FxAppOptions{
				DB:           testDBConnector{},
				QueueBackend: &testQueueBackend{},
				TaskExecutor: NewCodeTaskExecutor(nil),
			},
		},
		{
			name: "missing backend",
			opts: FxAppOptions{
				DB:           testDBConnector{},
				Logger:       testLogger(),
				TaskExecutor: NewCodeTaskExecutor(nil),
			},
		},
		{
			name: "missing executor",
			opts: FxAppOptions{
				DB:           testDBConnector{},
				Logger:       testLogger(),
				QueueBackend: &testQueueBackend{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic")
				}
			}()
			_ = NewFxApp(tt.opts)
		})
	}
}
