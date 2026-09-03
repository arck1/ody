package functional_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"schedulor"
	"schedulor/execution"
	"schedulor/task"
	"schedulor/worker"
	workerfx "schedulor/worker/fx"
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
	registry, err := task.NewRegistry(module)
	s.Require().NoError(err)
	store := execution.NewMemoryStore()
	created, err := definition.Enqueue(context.Background(), store, 41)
	s.Require().NoError(err)
	runner, err := worker.New(store, registry, nil, nil, fastWorkerOptions())
	s.Require().NoError(err)
	lifecycle, err := workerfx.New(runner)
	s.Require().NoError(err)
	app, err := schedulor.NewFxApp(schedulor.FxAppOptions{
		Components: []schedulor.FxLifecycleComponent{lifecycle},
		Options:    []fx.Option{fx.NopLogger},
	})
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.Require().NoError(app.Start(ctx))
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { s.Require().NoError(app.Stop(ctx)) }) }
	s.T().Cleanup(stop)

	completed := awaitExecution(s.T(), store, created.ID, execution.StatusSucceeded)
	s.Equal(1, completed.Attempt)
	stop()
}
