package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/arck1/ody/execution"
)

func newTestStore(t *testing.T) (*Store, func()) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	store, err := New(client, Options{Prefix: "test:{count}:"})
	require.NoError(t, err)
	return store, func() { require.NoError(t, client.Close()) }
}

func TestStoreExecutionLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store, err := New(client, Options{})
	require.NoError(t, err)
	ctx := context.Background()

	created, err := store.CreateExecution(ctx, execution.CreateExecution{
		TaskName: "work", TaskVersion: 1, Input: []byte(`{"value":21}`), MaxAttempts: 2, IdempotencyKey: "same",
	})
	require.NoError(t, err)
	duplicate, err := store.CreateExecution(ctx, execution.CreateExecution{TaskName: "work", IdempotencyKey: "same"})
	require.NoError(t, err)
	require.Equal(t, created.ID, duplicate.ID)

	claimed, err := store.Claim(ctx, "worker", []execution.TaskKey{{Name: "work", Version: 1}}, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, execution.StatusRunning, claimed[0].Status)
	require.ErrorIs(t, store.Succeed(ctx, created.ID, created.LeaseToken, nil), execution.ErrLeaseLost)
	require.NoError(t, store.Succeed(ctx, created.ID, claimed[0].LeaseToken, []byte(`{"value":42}`)))

	completed, err := store.GetExecution(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, execution.StatusSucceeded, completed.Status)
	require.JSONEq(t, `{"value":42}`, string(completed.Output))
	events, err := store.Events(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, []execution.EventType{execution.EventCreated, execution.EventStarted, execution.EventSucceeded}, eventTypes(events))

	restarted, err := store.RestartExecution(ctx, created.ID, time.Time{})
	require.NoError(t, err)
	require.Equal(t, execution.StatusPending, restarted.Status)
	require.Zero(t, restarted.Attempt)
}

func TestStoreRetryAndPipelineLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store, err := New(client, Options{Prefix: "test:{execution}:"})
	require.NoError(t, err)
	ctx := context.Background()

	run, err := store.CreatePipelineRun(ctx, execution.CreatePipelineRun{PipelineName: "flow", PipelineVersion: 1, Input: []byte(`{}`)})
	require.NoError(t, err)
	require.NoError(t, store.SetPipelineRunStatus(ctx, run.ID, run.Revision, execution.RunRunning, ""))
	created, err := store.CreateExecution(ctx, execution.CreateExecution{
		TaskName: "step", TaskVersion: 1, MaxAttempts: 2, PipelineRunID: &run.ID, NodeKey: "first",
	})
	require.NoError(t, err)
	claimed, err := store.Claim(ctx, "worker", []execution.TaskKey{{Name: "step", Version: 1}}, 1, time.Second)
	require.NoError(t, err)
	require.NoError(t, store.Retry(ctx, created.ID, claimed[0].LeaseToken, "temporary", -time.Second))
	claimed, err = store.Claim(ctx, "worker", []execution.TaskKey{{Name: "step", Version: 1}}, 1, time.Second)
	require.NoError(t, err)
	require.NoError(t, store.Retry(ctx, created.ID, claimed[0].LeaseToken, "exhausted", 0))
	failed, err := store.GetExecution(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, execution.StatusFailed, failed.Status)
	require.ErrorIs(t, store.Heartbeat(ctx, created.ID, claimed[0].LeaseToken, time.Second), execution.ErrLeaseLost)

	currentRun, err := store.GetPipelineRun(ctx, run.ID)
	require.NoError(t, err)
	require.NoError(t, store.SetPipelineRunStatus(ctx, run.ID, currentRun.Revision, execution.RunFailed, "step failed"))
	storedRun, err := store.GetPipelineRun(ctx, run.ID)
	require.NoError(t, err)
	require.Equal(t, execution.RunFailed, storedRun.Status)
	require.NotNil(t, storedRun.FinishedAt)
	items, err := store.ListRunExecutions(ctx, run.ID)
	require.NoError(t, err)
	require.Len(t, items, 1)
	_, err = store.GetExecution(ctx, run.ID)
	require.ErrorIs(t, err, execution.ErrNotFound)
}

func TestStoreReapsExpiredFinalLease(t *testing.T) {
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store, err := New(client, Options{})
	require.NoError(t, err)
	ctx := context.Background()
	created, err := store.CreateExecution(ctx, execution.CreateExecution{TaskName: "work", MaxAttempts: 1})
	require.NoError(t, err)
	claimed, err := store.Claim(ctx, "worker", []execution.TaskKey{{Name: "work", Version: 0}}, 1, -time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	runs, err := store.ReapExpired(ctx)
	require.NoError(t, err)
	require.Empty(t, runs)
	item, err := store.GetExecution(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, execution.StatusFailed, item.Status)
	require.Equal(t, "lease expired after maximum attempts", item.LastError)
}

func TestStoreMaintainsBoundedStatistics(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	created, err := store.CreateExecution(ctx, execution.CreateExecution{TaskName: "counted", TaskVersion: 1})
	require.NoError(t, err)
	claimed, err := store.Claim(ctx, "worker", []execution.TaskKey{{Name: "counted", Version: 1}}, 1, time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.Succeed(ctx, created.ID, claimed[0].LeaseToken, json.RawMessage(`1`)))
	run, err := store.CreatePipelineRun(ctx, execution.CreatePipelineRun{PipelineName: "counted-flow", PipelineVersion: 1})
	require.NoError(t, err)
	require.NoError(t, store.SetPipelineRunStatus(ctx, run.ID, run.Revision, execution.RunRunning, ""))

	executionCounts, err := store.ExecutionCounts(ctx)
	require.NoError(t, err)
	require.Equal(t, []execution.ExecutionCount{{TaskName: "counted", Status: execution.StatusSucceeded, Count: 1}}, executionCounts)
	pipelineCounts, err := store.PipelineCounts(ctx)
	require.NoError(t, err)
	require.Equal(t, []execution.PipelineCount{{PipelineName: "counted-flow", Status: execution.RunRunning, Count: 1}}, pipelineCounts)
}

func TestStorePurgesTerminalHistoryAndIndexes(t *testing.T) {
	store, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	created, err := store.CreateExecution(ctx, execution.CreateExecution{TaskName: "purge", TaskVersion: 1, IdempotencyKey: "old"})
	require.NoError(t, err)
	claimed, err := store.Claim(ctx, "worker", []execution.TaskKey{{Name: "purge", Version: 1}}, 1, time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.Succeed(ctx, created.ID, claimed[0].LeaseToken, nil))

	result, err := store.Purge(ctx, time.Now().UTC().Add(time.Second), 10)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Executions)
	_, err = store.GetExecution(ctx, created.ID)
	require.ErrorIs(t, err, execution.ErrNotFound)
	recreated, err := store.CreateExecution(ctx, execution.CreateExecution{TaskName: "purge", TaskVersion: 1, IdempotencyKey: "old"})
	require.NoError(t, err)
	require.NotEqual(t, created.ID, recreated.ID)
}

func eventTypes(events []execution.Event) []execution.EventType {
	result := make([]execution.EventType, len(events))
	for index, event := range events {
		result[index] = event.Type
	}
	return result
}
