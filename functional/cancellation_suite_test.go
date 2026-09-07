package functional_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/arck1/ody/execution"
	"github.com/arck1/ody/pipeline"
	"github.com/arck1/ody/task"
	"github.com/arck1/ody/worker"
)

type CancellationSuite struct{ suite.Suite }

func TestCancellationSuite(t *testing.T) {
	suite.Run(t, new(CancellationSuite))
}

func (s *CancellationSuite) TestCancelledPipelineNeverRunsPendingTask() {
	definition := task.New[string, string]("functional.cancel")
	var called atomic.Bool
	module, err := task.NewModule("cancellation", task.Handle(definition, func(_ context.Context, message task.Message[string]) (string, error) {
		called.Store(true)
		return message.Input, nil
	}))
	s.Require().NoError(err)
	tasks, err := task.NewRegistry(module)
	s.Require().NoError(err)
	flow := pipeline.New[string]("functional-cancellation", 1)
	pipeline.Start(flow, "work", definition, func(input string) string { return input })
	pipelines, err := pipeline.NewRegistry(flow)
	s.Require().NoError(err)
	store := execution.NewMemoryStore()
	engine, err := pipeline.NewEngine(store, pipelines)
	s.Require().NoError(err)
	run, err := pipeline.Run(context.Background(), engine, flow, "payload")
	s.Require().NoError(err)
	s.Require().NoError(engine.Cancel(context.Background(), run.ID, "user request"))
	runner, err := worker.New(store, tasks, engine, nil, fastWorkerOptions())
	s.Require().NoError(err)
	stop := startWorker(s.T(), runner)
	s.Never(called.Load, 20*time.Millisecond, time.Millisecond)
	requireWorkerStopped(s.T(), stop)

	snapshot, err := engine.Inspect(context.Background(), run.ID)
	s.Require().NoError(err)
	s.Equal(execution.RunCancelled, snapshot.Run.Status)
	s.Require().Len(snapshot.Executions, 1)
	s.Equal(execution.StatusCancelled, snapshot.Executions[0].Status)
	requireEventTypes(s.T(), store, snapshot.Executions[0].ID,
		execution.EventCreated,
		execution.EventCancelled,
	)
}
