// Package prometheus exposes worker transitions and durable queue state as Prometheus metrics.
package prometheus

import (
	"context"
	"errors"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"

	"ody/execution"
	"ody/worker"
)

type Metrics struct {
	transitions          *prom.CounterVec
	duration             *prom.HistogramVec
	errors               *prom.CounterVec
	infrastructureErrors *prom.CounterVec
}

// New registers transition metrics and a collector for current persisted state.
func New(registerer prom.Registerer, store execution.StatisticsReader) (*Metrics, error) {
	if registerer == nil {
		return nil, errors.New("prometheus registerer is nil")
	}
	if store == nil {
		return nil, errors.New("prometheus execution store is nil")
	}
	metrics := &Metrics{
		transitions: prom.NewCounterVec(prom.CounterOpts{
			Namespace: "ody", Subsystem: "worker", Name: "task_transitions_total",
			Help: "Number of task state transitions observed by workers.",
		}, []string{"task", "status"}),
		duration: prom.NewHistogramVec(prom.HistogramOpts{
			Namespace: "ody", Subsystem: "worker", Name: "task_duration_seconds",
			Help:    "Task processing duration from first start to terminal transition.",
			Buckets: prom.DefBuckets,
		}, []string{"task", "status"}),
		errors: prom.NewCounterVec(prom.CounterOpts{
			Namespace: "ody", Subsystem: "worker", Name: "observer_errors_total",
			Help: "Number of worker transition errors reported to the observer.",
		}, []string{"task"}),
		infrastructureErrors: prom.NewCounterVec(prom.CounterOpts{
			Namespace: "ody", Subsystem: "worker", Name: "infrastructure_errors_total",
			Help: "Number of worker infrastructure errors by operation.",
		}, []string{"operation"}),
	}
	for _, collector := range []prom.Collector{metrics.transitions, metrics.duration, metrics.errors, metrics.infrastructureErrors, newStoreCollector(store)} {
		if err := registerer.Register(collector); err != nil {
			return nil, err
		}
	}
	return metrics, nil
}

// InfrastructureError implements worker.Observer.
func (m *Metrics) InfrastructureError(_ context.Context, operation worker.Operation, _ error) {
	m.infrastructureErrors.WithLabelValues(string(operation)).Inc()
}

// Transition implements worker.Observer.
func (m *Metrics) Transition(_ context.Context, item execution.Execution, status execution.Status, transitionErr error) {
	m.transitions.WithLabelValues(item.TaskName, string(status)).Inc()
	if transitionErr != nil {
		m.errors.WithLabelValues(item.TaskName).Inc()
	}
	if status == execution.StatusSucceeded || status == execution.StatusFailed || status == execution.StatusCancelled {
		started := item.CreatedAt
		if item.StartedAt != nil {
			started = *item.StartedAt
		}
		duration := time.Since(started).Seconds()
		if duration < 0 {
			duration = 0
		}
		m.duration.WithLabelValues(item.TaskName, string(status)).Observe(duration)
	}
}

type storeCollector struct {
	store          execution.StatisticsReader
	executionsDesc *prom.Desc
	pipelinesDesc  *prom.Desc
	scrapeErrors   *prom.Desc
}

func newStoreCollector(store execution.StatisticsReader) *storeCollector {
	return &storeCollector{
		store:          store,
		executionsDesc: prom.NewDesc("ody_store_task_executions", "Current persisted task executions by task and status.", []string{"task", "status"}, nil),
		pipelinesDesc:  prom.NewDesc("ody_store_pipeline_runs", "Current persisted pipeline runs by pipeline and status.", []string{"pipeline", "status"}, nil),
		scrapeErrors:   prom.NewDesc("ody_store_scrape_error", "Whether the latest durable state scrape failed.", nil, nil),
	}
}

func (c *storeCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.executionsDesc
	ch <- c.pipelinesDesc
	ch <- c.scrapeErrors
}

func (c *storeCollector) Collect(ch chan<- prom.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tasks, taskErr := c.store.ExecutionCounts(ctx)
	runs, runErr := c.store.PipelineCounts(ctx)
	if taskErr != nil || runErr != nil {
		ch <- prom.MustNewConstMetric(c.scrapeErrors, prom.GaugeValue, 1)
		return
	}
	ch <- prom.MustNewConstMetric(c.scrapeErrors, prom.GaugeValue, 0)
	for _, item := range tasks {
		ch <- prom.MustNewConstMetric(c.executionsDesc, prom.GaugeValue, float64(item.Count), item.TaskName, string(item.Status))
	}
	for _, run := range runs {
		ch <- prom.MustNewConstMetric(c.pipelinesDesc, prom.GaugeValue, float64(run.Count), run.PipelineName, string(run.Status))
	}
}
