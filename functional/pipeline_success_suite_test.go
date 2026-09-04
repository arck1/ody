package functional_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"schedulor/execution"
	"schedulor/pipeline"
	"schedulor/task"
	"schedulor/worker"
)

type PipelineSuccessSuite struct{ suite.Suite }

func TestPipelineSuccessSuite(t *testing.T) {
	suite.Run(t, new(PipelineSuccessSuite))
}

func (s *PipelineSuccessSuite) TestFanOutFanInUsesStoredOutputs() {
	type order struct {
		Price    int
		Quantity int
		Discount int
	}
	type totalInput struct{ Subtotal, Discount int }

	subtotalTask := task.New[order, int]("functional.subtotal")
	discountTask := task.New[order, int]("functional.discount")
	totalTask := task.New[totalInput, int]("functional.total")
	module, err := task.NewModule("checkout",
		task.Handle(subtotalTask, func(_ context.Context, message task.Message[order]) (int, error) {
			return message.Input.Price * message.Input.Quantity, nil
		}),
		task.Handle(discountTask, func(_ context.Context, message task.Message[order]) (int, error) {
			return message.Input.Discount, nil
		}),
		task.Handle(totalTask, func(_ context.Context, message task.Message[totalInput]) (int, error) {
			return message.Input.Subtotal - message.Input.Discount, nil
		}),
	)
	s.Require().NoError(err)
	tasks, err := task.NewRegistry(module)
	s.Require().NoError(err)
	flow := pipeline.New[order]("functional-checkout", 1)
	subtotal := pipeline.Start(flow, "subtotal", subtotalTask, func(value order) order { return value })
	discount := pipeline.Start(flow, "discount", discountTask, func(value order) order { return value })
	total := pipeline.Join2(flow, subtotal, discount, "total", totalTask, func(left, right int) totalInput {
		return totalInput{Subtotal: left, Discount: right}
	})
	pipelines, err := pipeline.NewRegistry(flow)
	s.Require().NoError(err)
	store := execution.NewMemoryStore()
	engine, err := pipeline.NewEngine(store, pipelines)
	s.Require().NoError(err)
	run, err := pipeline.Run(context.Background(), engine, flow, order{Price: 10, Quantity: 4, Discount: 3})
	s.Require().NoError(err)
	runner, err := worker.New(store, tasks, engine, nil, worker.Options{
		Concurrency:       2,
		PollInterval:      time.Millisecond,
		LeaseDuration:     time.Second,
		HeartbeatInterval: 100 * time.Millisecond,
	})
	s.Require().NoError(err)
	stop := startWorker(s.T(), runner)
	s.T().Cleanup(stop)

	awaitPipeline(s.T(), store, run.ID, execution.RunSucceeded)
	result, err := pipeline.Output(context.Background(), store, run.ID, total)
	s.Require().NoError(err)
	s.Equal(37, result)
	snapshot, err := engine.Inspect(context.Background(), run.ID)
	s.Require().NoError(err)
	s.Len(snapshot.Executions, 3)
}
