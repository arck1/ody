//go:build integration

package functional_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"

	"schedulor/execution"
	executionpostgres "schedulor/execution/postgres"
	"schedulor/monitoring"
	monitoringprom "schedulor/monitoring/prometheus"
	"schedulor/pipeline"
	"schedulor/task"
	"schedulor/worker"
)

type PostgresInfrastructureSuite struct {
	suite.Suite
	db    *sql.DB
	store *executionpostgres.Store
}

func TestPostgresInfrastructureSuite(t *testing.T) {
	suite.Run(t, new(PostgresInfrastructureSuite))
}

func (s *PostgresInfrastructureSuite) SetupSuite() {
	s.db, s.store = startPostgres(s.T())
}

func (s *PostgresInfrastructureSuite) SetupTest() {
	_, err := s.db.ExecContext(context.Background(), `TRUNCATE execution_events, task_executions, pipeline_runs`)
	s.Require().NoError(err)
}

func (s *PostgresInfrastructureSuite) TestWorkerHistoryMetricsAndRestartRoundTrip() {
	type input struct{ Value int }
	type output struct{ Value int }
	definition := task.New[input, output]("infra.double", task.WithMaxAttempts(2))
	var calls atomic.Int32
	module, err := task.NewModule("infra", task.Handle(definition, func(_ context.Context, message task.Message[input]) (output, error) {
		calls.Add(1)
		return output{Value: message.Input.Value * 2}, nil
	}))
	s.Require().NoError(err)
	registry, err := task.NewRegistry(module)
	s.Require().NoError(err)
	metricsRegistry := prom.NewRegistry()
	metrics, err := monitoringprom.New(metricsRegistry, s.store)
	s.Require().NoError(err)
	runner, err := worker.New(s.store, registry, nil, metrics, worker.Options{
		Concurrency: 2, PollInterval: 5 * time.Millisecond, LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
	})
	s.Require().NoError(err)
	created, err := definition.Enqueue(context.Background(), s.store, input{Value: 21}, task.WithIdempotencyKey("double:21"))
	s.Require().NoError(err)
	duplicate, err := definition.Enqueue(context.Background(), s.store, input{Value: 21}, task.WithIdempotencyKey("double:21"))
	s.Require().NoError(err)
	s.Equal(created.ID, duplicate.ID)
	stop := startWorker(s.T(), runner)
	defer stop()

	completed := awaitExecution(s.T(), s.store, created.ID, execution.StatusSucceeded)
	var result output
	s.Require().NoError(json.Unmarshal(completed.Output, &result))
	s.Equal(42, result.Value)
	service, err := monitoring.New(s.store)
	s.Require().NoError(err)
	details, err := service.Task(context.Background(), created.ID)
	s.Require().NoError(err)
	s.Len(details.Events, 3)

	_, err = service.RestartTask(context.Background(), created.ID)
	s.Require().NoError(err)
	completed = awaitExecution(s.T(), s.store, created.ID, execution.StatusSucceeded)
	s.Equal(1, completed.Attempt)
	s.Equal(int32(2), calls.Load())
	details, err = service.Task(context.Background(), created.ID)
	s.Require().NoError(err)
	s.Equal(execution.EventRestarted, details.Events[3].Type)
	s.Len(details.Events, 6)
	families, err := metricsRegistry.Gather()
	s.Require().NoError(err)
	s.NotEmpty(families)
}

func (s *PostgresInfrastructureSuite) TestPersistentPipelineFanOutAndJoin() {
	type order struct{ Price, Quantity, Discount int }
	type totalInput struct{ Subtotal, Discount int }
	subtotalTask := task.New[order, int]("infra.subtotal")
	discountTask := task.New[order, int]("infra.discount")
	totalTask := task.New[totalInput, int]("infra.total")
	module, err := task.NewModule("checkout",
		task.Handle(subtotalTask, func(_ context.Context, value task.Message[order]) (int, error) {
			return value.Input.Price * value.Input.Quantity, nil
		}),
		task.Handle(discountTask, func(_ context.Context, value task.Message[order]) (int, error) { return value.Input.Discount, nil }),
		task.Handle(totalTask, func(_ context.Context, value task.Message[totalInput]) (int, error) {
			return value.Input.Subtotal - value.Input.Discount, nil
		}),
	)
	s.Require().NoError(err)
	tasks, err := task.NewRegistry(module)
	s.Require().NoError(err)
	flow := pipeline.New[order]("infra-checkout", 1)
	subtotal := pipeline.Start(flow, "subtotal", subtotalTask, func(value order) order { return value })
	discount := pipeline.Start(flow, "discount", discountTask, func(value order) order { return value })
	total := pipeline.Join2(flow, subtotal, discount, "total", totalTask, func(left, right int) totalInput {
		return totalInput{Subtotal: left, Discount: right}
	})
	pipelines, err := pipeline.NewRegistry(flow)
	s.Require().NoError(err)
	engine, err := pipeline.NewEngine(s.store, pipelines)
	s.Require().NoError(err)
	run, err := pipeline.Run(context.Background(), engine, flow, order{Price: 10, Quantity: 4, Discount: 3})
	s.Require().NoError(err)
	runner, err := worker.New(s.store, tasks, engine, nil, worker.Options{
		Concurrency: 3, PollInterval: 5 * time.Millisecond, LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
	})
	s.Require().NoError(err)
	stop := startWorker(s.T(), runner)
	defer stop()

	awaitPipeline(s.T(), s.store, run.ID, execution.RunSucceeded)
	result, err := pipeline.Output(context.Background(), s.store, run.ID, total)
	s.Require().NoError(err)
	s.Equal(37, result)
	snapshot, err := engine.Inspect(context.Background(), run.ID)
	s.Require().NoError(err)
	s.Len(snapshot.Executions, 3)
}
