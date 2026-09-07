package functional_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"

	"ody/execution"
	"ody/pipeline"
	"ody/task"
	"ody/worker"
)

type PipelineFailureSuite struct{ suite.Suite }

func TestPipelineFailureSuite(t *testing.T) {
	suite.Run(t, new(PipelineFailureSuite))
}

func (s *PipelineFailureSuite) TestPermanentErrorDoesNotScheduleDownstream() {
	load := task.New[string, string]("functional.load")
	validate := task.New[string, string]("functional.validate")
	publish := task.New[string, string]("functional.publish")
	var published atomic.Bool
	module, err := task.NewModule("publication",
		task.Handle(load, func(_ context.Context, message task.Message[string]) (string, error) {
			return "loaded:" + message.Input, nil
		}),
		task.Handle(validate, func(context.Context, task.Message[string]) (string, error) {
			return "", task.Permanent(errors.New("document is invalid"))
		}),
		task.Handle(publish, func(_ context.Context, message task.Message[string]) (string, error) {
			published.Store(true)
			return message.Input, nil
		}),
	)
	s.Require().NoError(err)
	tasks, err := task.NewRegistry(module)
	s.Require().NoError(err)
	flow := pipeline.New[string]("functional-publication", 1)
	loaded := pipeline.Start(flow, "load", load, func(input string) string { return input })
	validated := pipeline.Then(flow, loaded, "validate", validate, func(output string) string { return output })
	pipeline.Then(flow, validated, "publish", publish, func(output string) string { return output })
	pipelines, err := pipeline.NewRegistry(flow)
	s.Require().NoError(err)
	store := execution.NewMemoryStore()
	engine, err := pipeline.NewEngine(store, pipelines)
	s.Require().NoError(err)
	run, err := pipeline.Run(context.Background(), engine, flow, "document")
	s.Require().NoError(err)
	runner, err := worker.New(store, tasks, engine, nil, fastWorkerOptions())
	s.Require().NoError(err)
	stop := startWorker(s.T(), runner)
	s.T().Cleanup(stop)

	failed := awaitPipeline(s.T(), store, run.ID, execution.RunFailed)
	s.Contains(failed.Error, "validate")
	s.Contains(failed.Error, "document is invalid")
	s.False(published.Load())
	snapshot, err := engine.Inspect(context.Background(), run.ID)
	s.Require().NoError(err)
	s.Len(snapshot.Executions, 2)
}
