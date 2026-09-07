package functional_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/arck1/ody"
	"github.com/arck1/ody/execution"
	"github.com/arck1/ody/schedule"
	"github.com/arck1/ody/task"
)

type SchedulerSuite struct{ suite.Suite }

func TestSchedulerSuite(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}

func (s *SchedulerSuite) TestAppRecoversLatestMisfireIntoWorkerQueue() {
	fired := make(chan time.Time, 1)
	definition := task.New[time.Time, struct{}]("functional.scheduled")
	module, err := task.NewModule("scheduled", task.Handle(definition, func(_ context.Context, message task.Message[time.Time]) (struct{}, error) {
		fired <- message.Input
		return struct{}{}, nil
	}))
	s.Require().NoError(err)
	cronDefinition, err := schedule.Task(
		"functional-hourly", "@hourly", definition, func(at time.Time) time.Time { return at },
		schedule.WithMisfire(schedule.MisfireLatest, 2*time.Hour, 1),
	)
	s.Require().NoError(err)
	store := execution.NewMemoryStore()
	app, err := ody.New(store,
		ody.Tasks(module),
		ody.Schedules(cronDefinition),
		ody.WithWorker(fastWorkerOptions()),
	)
	s.Require().NoError(err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	select {
	case scheduledAt := <-fired:
		s.False(scheduledAt.IsZero())
	case <-time.After(2 * time.Second):
		s.Fail("scheduled task was not executed")
	}
	cancel()
	s.Require().ErrorIs(<-done, context.Canceled)
	items, err := store.ListExecutions(context.Background(), execution.ListFilter{TaskName: definition.Name()})
	s.Require().NoError(err)
	s.Require().Len(items, 1)
	s.Contains(items[0].IdempotencyKey, "schedule:functional-hourly:")
}
