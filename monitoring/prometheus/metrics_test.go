package prometheus

import (
	"context"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"schedulor/execution"
)

func TestMetricsExposeTransitionsAndStoreState(t *testing.T) {
	store := execution.NewMemoryStore()
	created, err := store.CreateExecution(context.Background(), execution.CreateExecution{TaskName: "email"})
	require.NoError(t, err)
	registry := prom.NewRegistry()
	metrics, err := New(registry, store)
	require.NoError(t, err)

	metrics.Transition(context.Background(), created, execution.StatusSucceeded, nil)

	families, err := registry.Gather()
	require.NoError(t, err)
	names := make(map[string]bool)
	for _, family := range families {
		names[family.GetName()] = true
		if family.GetName() == "schedulor_worker_task_transitions_total" {
			require.InDelta(t, 1, family.Metric[0].Counter.GetValue(), 0)
		}
	}
	require.True(t, names["schedulor_store_task_executions"])
	require.True(t, names["schedulor_worker_task_transitions_total"])
}
