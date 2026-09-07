package functional_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"ody/execution"
	"ody/task"
	"ody/worker"
)

type RetrySuite struct{ suite.Suite }

func TestRetrySuite(t *testing.T) {
	suite.Run(t, new(RetrySuite))
}

func (s *RetrySuite) TestDelayedTaskHonorsRetryAfterAndSucceeds() {
	definition := task.New[int, int](
		"functional.delayed-retry",
		task.WithMaxAttempts(3),
		task.WithRetryPolicy(func(int) time.Duration { return time.Hour }),
	)
	var calls atomic.Int32
	var callMu sync.Mutex
	var callTimes []time.Time
	module, err := task.NewModule("retry", task.Handle(definition, func(_ context.Context, message task.Message[int]) (int, error) {
		callMu.Lock()
		callTimes = append(callTimes, time.Now())
		callMu.Unlock()
		if calls.Add(1) == 1 {
			return 0, task.RetryAfter(errors.New("temporary outage"), 40*time.Millisecond)
		}
		return message.Input * 2, nil
	}))
	s.Require().NoError(err)
	registry, err := task.NewRegistry(module)
	s.Require().NoError(err)
	store := execution.NewMemoryStore()
	availableAt := time.Now().Add(50 * time.Millisecond)
	created, err := definition.Enqueue(context.Background(), store, 21, task.WithAvailableAt(availableAt))
	s.Require().NoError(err)
	runner, err := worker.New(store, registry, nil, nil, fastWorkerOptions())
	s.Require().NoError(err)
	stop := startWorker(s.T(), runner)
	s.T().Cleanup(stop)

	completed := awaitExecution(s.T(), store, created.ID, execution.StatusSucceeded)
	s.Equal(2, completed.Attempt)
	s.Equal(int32(2), calls.Load())
	callMu.Lock()
	times := append([]time.Time(nil), callTimes...)
	callMu.Unlock()
	s.Require().Len(times, 2)
	s.False(times[0].Before(availableAt))
	s.GreaterOrEqual(times[1].Sub(times[0]), 30*time.Millisecond)
	requireEventTypes(s.T(), store, created.ID,
		execution.EventCreated,
		execution.EventStarted,
		execution.EventRetried,
		execution.EventStarted,
		execution.EventSucceeded,
	)
}
