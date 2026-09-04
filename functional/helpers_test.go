package functional_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"schedulor/execution"
	"schedulor/worker"
)

func fastWorkerOptions() worker.Options {
	return worker.Options{
		PollInterval:      time.Millisecond,
		LeaseDuration:     time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
	}
}

func startWorker(t *testing.T, runner *worker.Worker) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				require.Fail(t, "worker did not stop")
			}
		})
	}
}

func awaitExecution(t *testing.T, store execution.Store, id uuid.UUID, status execution.Status) execution.Execution {
	t.Helper()
	var result execution.Execution
	require.Eventually(t, func() bool {
		item, err := store.GetExecution(context.Background(), id)
		result = item
		return err == nil && item.Status == status
	}, 2*time.Second, time.Millisecond)
	return result
}

func awaitPipeline(t *testing.T, store execution.Store, id uuid.UUID, status execution.RunStatus) execution.PipelineRun {
	t.Helper()
	var result execution.PipelineRun
	require.Eventually(t, func() bool {
		run, err := store.GetPipelineRun(context.Background(), id)
		result = run
		return err == nil && run.Status == status
	}, 2*time.Second, time.Millisecond)
	return result
}

func requireEventTypes(t *testing.T, store execution.Store, id uuid.UUID, expected ...execution.EventType) {
	t.Helper()
	events, err := store.Events(context.Background(), id)
	require.NoError(t, err)
	actual := make([]execution.EventType, len(events))
	for index, event := range events {
		actual[index] = event.Type
	}
	require.Equal(t, expected, actual)
}

func requireWorkerStopped(t *testing.T, stop func()) {
	t.Helper()
	if stop == nil {
		require.Fail(t, "worker stop function is nil")
	}
	stop()
}
