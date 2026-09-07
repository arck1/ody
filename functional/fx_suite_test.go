package functional_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"schedulor"
	"schedulor/execution"
	"schedulor/task"
)

type FxLifecycleSuite struct{ suite.Suite }

func TestFxLifecycleSuite(t *testing.T) {
	suite.Run(t, new(FxLifecycleSuite))
}

func (s *FxLifecycleSuite) TestWorkerRunsAndStopsThroughFx() {
	definition := task.New[int, int]("functional.fx")
	module, err := task.NewModule("fx", task.Handle(definition, func(_ context.Context, message task.Message[int]) (int, error) {
		return message.Input + 1, nil
	}))
	s.Require().NoError(err)
	store := execution.NewMemoryStore()
	var runtime *schedulor.App
	app := fx.New(
		fx.Supply(fx.Annotate(store, fx.As(new(execution.Store)))),
		schedulor.FxModule(schedulor.Tasks(module), schedulor.WithWorker(fastWorkerOptions())),
		fx.Populate(&runtime),
		fx.NopLogger,
	)
	created, err := definition.Enqueue(context.Background(), runtime, 41)
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.Require().NoError(app.Start(ctx))
	s.T().Cleanup(func() { _ = app.Stop(context.Background()) })

	completed := awaitExecution(s.T(), store, created.ID, execution.StatusSucceeded)
	s.Equal(1, completed.Attempt)
	s.Require().NoError(app.Stop(ctx))
}
