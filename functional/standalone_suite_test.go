package functional_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/arck1/ody/execution"
	"github.com/arck1/ody/task"
	"github.com/arck1/ody/worker"
)

type StandaloneTaskSuite struct{ suite.Suite }

func TestStandaloneTaskSuite(t *testing.T) {
	suite.Run(t, new(StandaloneTaskSuite))
}

func (s *StandaloneTaskSuite) TestPersistsResultIdempotencyAndHistory() {
	type input struct{ Name string }
	type output struct{ Greeting string }

	definition := task.New[input, output]("functional.greet")
	module, err := task.NewModule("greeting", task.Handle(definition, func(_ context.Context, message task.Message[input]) (output, error) {
		return output{Greeting: "Hello, " + message.Input.Name}, nil
	}))
	s.Require().NoError(err)
	registry, err := task.NewRegistry(module)
	s.Require().NoError(err)
	store := execution.NewMemoryStore()
	created, err := definition.Enqueue(context.Background(), store, input{Name: "Ada"}, task.WithIdempotencyKey("greet:ada"))
	s.Require().NoError(err)
	duplicate, err := definition.Enqueue(context.Background(), store, input{Name: "Ada"}, task.WithIdempotencyKey("greet:ada"))
	s.Require().NoError(err)
	s.Equal(created.ID, duplicate.ID)

	runner, err := worker.New(store, registry, nil, nil, fastWorkerOptions())
	s.Require().NoError(err)
	stop := startWorker(s.T(), runner)
	s.T().Cleanup(stop)

	completed := awaitExecution(s.T(), store, created.ID, execution.StatusSucceeded)
	var result output
	s.Require().NoError(json.Unmarshal(completed.Output, &result))
	s.Equal("Hello, Ada", result.Greeting)
	requireEventTypes(s.T(), store, created.ID,
		execution.EventCreated,
		execution.EventStarted,
		execution.EventSucceeded,
	)
}
