package schedule

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arck1/ody/execution"
	"github.com/arck1/ody/pipeline"
	"github.com/arck1/ody/task"
)

func TestTaskTickIsDurablyIdempotent(t *testing.T) {
	definition := task.New[time.Time, struct{}]("scheduled.task")
	cronDefinition, err := Task("hourly", "@hourly", definition, func(at time.Time) time.Time { return at })
	require.NoError(t, err)
	store := execution.NewMemoryStore()
	scheduler, err := New(store, nil, []Definition{cronDefinition}, Options{})
	require.NoError(t, err)
	scheduledAt := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)

	scheduler.fire(context.Background(), cronDefinition, scheduledAt)
	scheduler.fire(context.Background(), cronDefinition, scheduledAt)
	items, err := store.ListExecutions(context.Background(), execution.ListFilter{})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "schedule:hourly:2026-09-05T10:00:00Z", items[0].IdempotencyKey)
}

func TestPipelineTickIsDurablyIdempotent(t *testing.T) {
	step := task.New[int, int]("scheduled.pipeline.step")
	flow := pipeline.New[int]("scheduled-pipeline", 1)
	pipeline.Start(flow, "step", step, func(value int) int { return value })
	registry, err := pipeline.NewRegistry(flow)
	require.NoError(t, err)
	store := execution.NewMemoryStore()
	engine, err := pipeline.NewEngine(store, registry)
	require.NoError(t, err)
	cronDefinition, err := Pipeline("pipeline-hourly", "@hourly", flow, func(time.Time) int { return 42 })
	require.NoError(t, err)
	scheduler, err := New(store, engine, []Definition{cronDefinition}, Options{})
	require.NoError(t, err)
	scheduledAt := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)

	scheduler.fire(context.Background(), cronDefinition, scheduledAt)
	scheduler.fire(context.Background(), cronDefinition, scheduledAt)
	runs, err := store.ListPipelineRuns(context.Background(), execution.RunFilter{})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, "schedule:pipeline-hourly:2026-09-05T10:00:00Z", runs[0].IdempotencyKey)
}

func TestOverlapSkipDoesNotCreateAnotherActiveExecution(t *testing.T) {
	definition := task.New[int, struct{}]("scheduled.exclusive")
	cronDefinition, err := Task("exclusive", "@hourly", definition, func(time.Time) int { return 1 }, WithOverlap(OverlapSkip))
	require.NoError(t, err)
	store := execution.NewMemoryStore()
	scheduler, err := New(store, nil, []Definition{cronDefinition}, Options{})
	require.NoError(t, err)

	scheduler.fire(context.Background(), cronDefinition, time.Now().UTC())
	scheduler.fire(context.Background(), cronDefinition, time.Now().UTC().Add(time.Hour))
	items, err := store.ListExecutions(context.Background(), execution.ListFilter{})
	require.NoError(t, err)
	require.Len(t, items, 1)
}
