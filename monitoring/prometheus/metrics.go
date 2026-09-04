// Package prometheus exposes worker transitions and durable queue state as Prometheus metrics.
package prometheus

import (
	"context"
	"errors"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"

	"schedulor/execution"
)

type Metrics struct {
	transitions *prom.CounterVec
	duration    *prom.HistogramVec
	errors      *prom.CounterVec
}

// New registers transition metrics and a collector for current persisted state.
func New(registerer prom.Registerer, store execution.Store) (*Metrics, error) {
	if registerer == nil {
		return nil, errors.New("prometheus registerer is nil")
	}
	if store == nil {
		return nil, errors.New("prometheus execution store is nil")
	}
	metrics := &Metrics{
		transitions: prom.NewCounterVec(prom.CounterOpts{
			Namespace: "schedulor", Subsystem: "worker", Name: "task_transitions_total",
			Help: "Number of task state transitions observed by workers.",
		}, []string{"task", "status"}),
		duration: prom.NewHistogramVec(prom.HistogramOpts{
			Namespace: "schedulor", Subsystem: "worker", Name: "task_duration_seconds",
			Help:    "Task processing duration from first start to terminal transition.",
			Buckets: prom.DefBuckets,
		}, []string{"task", "status"}),
		errors: prom.NewCounterVec(prom.CounterOpts{
			Namespace: "schedulor", Subsystem: "worker", Name: "observer_errors_total",
			Help: "Number of worker transition errors reported to the observer.",
		}, []string{"task"}),
	}
	for _, collector := range []prom.Collector{metrics.transitions, metrics.duration, metrics.errors, newStoreCollector(store)} {
		if err := registerer.Register(collector); err != nil {
			return nil, err
		}
	}
	return metrics, nil
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
	store          execution.Store
	executionsDesc *prom.Desc
	pipelinesDesc  *prom.Desc
	scrapeErrors   *prom.Desc
}

func newStoreCollector(store execution.Store) *storeCollector {
	return &storeCollector{
		store:          store,
		executionsDesc: prom.NewDesc("schedulor_store_task_executions", "Current persisted task executions by task and status.", []string{"task", "status"}, nil),
		pipelinesDesc:  prom.NewDesc("schedulor_store_pipeline_runs", "Current persisted pipeline runs by pipeline and status.", []string{"pipeline", "status"}, nil),
		scrapeErrors:   prom.NewDesc("schedulor_store_scrape_error", "Whether the latest durable state scrape failed.", nil, nil),
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
	tasks, taskErr := c.store.ListExecutions(ctx, execution.ListFilter{})
	runs, runErr := c.store.ListPipelineRuns(ctx, nil)
	if taskErr != nil || runErr != nil {
		ch <- prom.MustNewConstMetric(c.scrapeErrors, prom.GaugeValue, 1)
		return
	}
	ch <- prom.MustNewConstMetric(c.scrapeErrors, prom.GaugeValue, 0)
	taskCounts := make(map[[2]string]float64)
	for _, item := range tasks {
		taskCounts[[2]string{item.TaskName, string(item.Status)}]++
	}
	for labels, count := range taskCounts {
		ch <- prom.MustNewConstMetric(c.executionsDesc, prom.GaugeValue, count, labels[0], labels[1])
	}
	runCounts := make(map[[2]string]float64)
	for _, run := range runs {
		runCounts[[2]string{run.PipelineName, string(run.Status)}]++
	}
	for labels, count := range runCounts {
		ch <- prom.MustNewConstMetric(c.pipelinesDesc, prom.GaugeValue, count, labels[0], labels[1])
	}
}
