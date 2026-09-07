//go:build integration

package functional_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	redislib "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"

	"github.com/arck1/ody/execution"
	executionredis "github.com/arck1/ody/execution/redis"
	"github.com/arck1/ody/monitoring"
	"github.com/arck1/ody/pipeline"
	"github.com/arck1/ody/task"
	"github.com/arck1/ody/worker"
)

type RedisInfrastructureSuite struct {
	suite.Suite
	client *redislib.Client
	store  *executionredis.Store
}

func TestRedisInfrastructureSuite(t *testing.T) {
	suite.Run(t, new(RedisInfrastructureSuite))
}

func (s *RedisInfrastructureSuite) SetupSuite() {
	s.client = startRedis(s.T())
	var err error
	s.store, err = executionredis.New(s.client, executionredis.Options{Prefix: "functional:{execution}:"})
	s.Require().NoError(err)
}

func (s *RedisInfrastructureSuite) SetupTest() {
	s.Require().NoError(s.client.FlushDB(context.Background()).Err())
}

func (s *RedisInfrastructureSuite) TestWorkerQueueHistoryRetryAndRestart() {
	type input struct{ Value int }
	type output struct{ Value int }
	definition := task.New[input, output]("redis.double", task.WithMaxAttempts(3))
	var calls atomic.Int32
	module, err := task.NewModule("redis", task.Handle(definition, func(_ context.Context, message task.Message[input]) (output, error) {
		if calls.Add(1) == 1 {
			return output{}, task.RetryAfter(context.DeadlineExceeded, 10*time.Millisecond)
		}
		return output{Value: message.Input.Value * 2}, nil
	}))
	s.Require().NoError(err)
	registry, err := task.NewRegistry(module)
	s.Require().NoError(err)
	runner, err := worker.New(s.store, registry, nil, nil, worker.Options{
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
	s.Equal(2, completed.Attempt)
	var result output
	s.Require().NoError(json.Unmarshal(completed.Output, &result))
	s.Equal(42, result.Value)
	details, err := monitoring.New(s.store)
	s.Require().NoError(err)
	tracked, err := details.Task(context.Background(), created.ID)
	s.Require().NoError(err)
	s.Equal([]execution.EventType{
		execution.EventCreated, execution.EventStarted, execution.EventRetried, execution.EventStarted, execution.EventSucceeded,
	}, eventTypes(tracked.Events))

	_, err = details.RestartTask(context.Background(), created.ID)
	s.Require().NoError(err)
	completed = awaitExecution(s.T(), s.store, created.ID, execution.StatusSucceeded)
	s.Equal(1, completed.Attempt)
	s.Equal(int32(3), calls.Load())
}

func (s *RedisInfrastructureSuite) TestPersistentPipelineUsesStoredResults() {
	type input struct{ Value int }
	first := task.New[input, int]("redis.first")
	second := task.New[int, int]("redis.second")
	module, err := task.NewModule("redis-pipeline",
		task.Handle(first, func(_ context.Context, message task.Message[input]) (int, error) { return message.Input.Value + 1, nil }),
		task.Handle(second, func(_ context.Context, message task.Message[int]) (int, error) { return message.Input * 2, nil }),
	)
	s.Require().NoError(err)
	registry, err := task.NewRegistry(module)
	s.Require().NoError(err)
	flow := pipeline.New[input]("redis-flow", 1)
	firstNode := pipeline.Start(flow, "first", first, func(value input) input { return value })
	lastNode := pipeline.Then(flow, firstNode, "second", second, func(value int) int { return value })
	pipelines, err := pipeline.NewRegistry(flow)
	s.Require().NoError(err)
	engine, err := pipeline.NewEngine(s.store, pipelines)
	s.Require().NoError(err)
	run, err := pipeline.Run(context.Background(), engine, flow, input{Value: 20})
	s.Require().NoError(err)
	runner, err := worker.New(s.store, registry, engine, nil, worker.Options{
		Concurrency: 2, PollInterval: 5 * time.Millisecond, LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
	})
	s.Require().NoError(err)
	stop := startWorker(s.T(), runner)
	defer stop()

	awaitPipeline(s.T(), s.store, run.ID, execution.RunSucceeded)
	result, err := pipeline.Output(context.Background(), s.store, run.ID, lastNode)
	s.Require().NoError(err)
	s.Equal(42, result)
	items, err := s.store.ListRunExecutions(context.Background(), run.ID)
	s.Require().NoError(err)
	s.Len(items, 2)
}

func eventTypes(events []execution.Event) []execution.EventType {
	result := make([]execution.EventType, len(events))
	for index, event := range events {
		result[index] = event.Type
	}
	return result
}
