package functional_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"

	"ody"
	"ody/execution"
	"ody/monitoring"
	"ody/pipeline"
	"ody/task"
)

type PipelineRestartSuite struct{ suite.Suite }

func TestPipelineRestartSuite(t *testing.T) {
	suite.Run(t, new(PipelineRestartSuite))
}

func (s *PipelineRestartSuite) TestRestartRecomputesDescendantsThroughPublicApp() {
	var source atomic.Int32
	source.Store(2)
	read := task.New[struct{}, int]("functional.restart.read")
	transform := task.New[int, int]("functional.restart.transform")
	module, err := task.NewModule("restart",
		task.Handle(read, func(context.Context, task.Message[struct{}]) (int, error) {
			return int(source.Load()), nil
		}),
		task.Handle(transform, func(_ context.Context, message task.Message[int]) (int, error) {
			return message.Input * 10, nil
		}),
	)
	s.Require().NoError(err)
	flow := pipeline.New[struct{}]("functional-restart", 1)
	first := pipeline.Start(flow, "read", read, func(struct{}) struct{} { return struct{}{} })
	last := pipeline.Then(flow, first, "transform", transform, func(value int) int { return value })
	store := execution.NewMemoryStore()
	app, err := ody.New(store,
		ody.Tasks(module),
		ody.Pipelines(flow),
		ody.WithWorker(fastWorkerOptions()),
	)
	s.Require().NoError(err)
	run, err := pipeline.Run(context.Background(), app.PipelineEngine(), flow, struct{}{})
	s.Require().NoError(err)
	stop := startWorker(s.T(), app.Worker())
	awaitPipeline(s.T(), store, run.ID, execution.RunSucceeded)
	stop()

	snapshot, err := app.PipelineEngine().Inspect(context.Background(), run.ID)
	s.Require().NoError(err)
	s.Require().Len(snapshot.Executions, 2)
	source.Store(3)
	service, err := monitoring.New(store, monitoring.WithPipelineRestarter(app))
	s.Require().NoError(err)
	_, err = service.RestartTask(context.Background(), snapshot.Executions[0].ID)
	s.Require().NoError(err)
	snapshot, err = app.PipelineEngine().Inspect(context.Background(), run.ID)
	s.Require().NoError(err)
	s.Equal(execution.StatusPending, snapshot.Executions[0].Status)
	s.Equal(execution.StatusBlocked, snapshot.Executions[1].Status)

	stop = startWorker(s.T(), app.Worker())
	s.T().Cleanup(stop)
	awaitPipeline(s.T(), store, run.ID, execution.RunSucceeded)
	result, err := pipeline.Output(context.Background(), store, run.ID, last)
	s.Require().NoError(err)
	s.Equal(30, result)
}
