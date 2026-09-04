//go:build integration

package functional_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"

	"schedulor"
	"schedulor/queue"
)

type RedisInfrastructureSuite struct {
	suite.Suite
	client *redis.Client
	queue  *queue.RedisQueue
}

type integrationLogger struct{}

func (integrationLogger) Debug(string, ...any) {}
func (integrationLogger) Info(string, ...any)  {}
func (integrationLogger) Warn(string, ...any)  {}
func (integrationLogger) Error(string, ...any) {}

func TestRedisInfrastructureSuite(t *testing.T) {
	suite.Run(t, new(RedisInfrastructureSuite))
}

func (s *RedisInfrastructureSuite) SetupSuite() {
	s.client = startRedis(s.T())
}

func (s *RedisInfrastructureSuite) SetupTest() {
	s.Require().NoError(s.client.FlushDB(context.Background()).Err())
	var err error
	s.queue, err = queue.NewRedisQueue(s.client, queue.RedisQueueOptions{
		Prefix: "functional:{queue}:", TaskMaxAttempts: 3, TaskVisibility: 500 * time.Millisecond,
	})
	s.Require().NoError(err)
}

func (s *RedisInfrastructureSuite) TestIdempotencyDelayLeaseRetryAndDLQ() {
	ctx := context.Background()
	payload := queue.JSONPayload(`{"invoice":"A-42"}`)
	availableAt := time.Now().UTC().Add(80 * time.Millisecond)
	first, err := s.queue.Enqueue(ctx, "billing", payload, availableAt, "invoice:A-42")
	s.Require().NoError(err)
	duplicate, err := s.queue.Enqueue(ctx, "billing", payload, availableAt, "invoice:A-42")
	s.Require().NoError(err)
	s.Equal(*first, *duplicate)
	claimed, err := s.queue.Claim(ctx, []string{"billing"}, 1)
	s.Require().NoError(err)
	s.Empty(claimed)
	s.Require().Eventually(func() bool {
		claimed, err = s.queue.Claim(ctx, []string{"billing"}, 1)
		return err == nil && len(claimed) == 1
	}, time.Second, 10*time.Millisecond)
	s.Equal(1, claimed[0].Attempts)
	ok, err := s.queue.Heartbeat(ctx, *first, claimed[0].LeaseToken)
	s.Require().NoError(err)
	s.True(ok)
	ok, err = s.queue.Nack(ctx, *first, claimed[0].LeaseToken, "temporary", 50*time.Millisecond)
	s.Require().NoError(err)
	s.True(ok)
	s.Require().Eventually(func() bool {
		claimed, err = s.queue.Claim(ctx, []string{"billing"}, 1)
		return err == nil && len(claimed) == 1
	}, time.Second, 10*time.Millisecond)
	s.Equal(2, claimed[0].Attempts)
	ok, err = s.queue.MoveToDLQ(ctx, *first, claimed[0].LeaseToken, "rejected")
	s.Require().NoError(err)
	s.True(ok)
	id := strconv.FormatInt(*first, 10)
	s.Equal("rejected", s.client.HGet(ctx, "functional:{queue}:dlq:"+id, "last_error").Val())
	s.Equal(int64(1), s.client.ZCard(ctx, "functional:{queue}:dlq").Val())
}

func (s *RedisInfrastructureSuite) TestLegacyExecutorProcessesAndAcknowledgesTask() {
	ctx := context.Background()
	var calls atomic.Int32
	executor := schedulor.NewCodeTaskExecutor([]schedulor.TaskHandler{{
		TaskName: "email",
		Handler: func(context.Context, map[string]any) error {
			calls.Add(1)
			return nil
		},
	}})
	runner, err := schedulor.NewLqExecutor(integrationLogger{}, s.queue, executor, &schedulor.LqExecutorOptions{
		PoolingTimeout: 5 * time.Millisecond, PoolingBatch: 1,
	})
	s.Require().NoError(err)
	id, err := s.queue.Enqueue(ctx, "email", queue.JSONPayload(`{"to":"user@example.com"}`), time.Now().UTC(), "email:1")
	s.Require().NoError(err)
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { runner.Run(workerCtx); close(done) }()
	s.Require().Eventually(func() bool { return calls.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
	s.Require().Eventually(func() bool {
		return s.client.Exists(ctx, "functional:{queue}:task:"+strconv.FormatInt(*id, 10)).Val() == 0
	}, time.Second, 10*time.Millisecond)
	cancel()
	s.Require().Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	recreated, err := s.queue.Enqueue(ctx, "email", queue.JSONPayload(`{}`), time.Now().UTC(), "email:1")
	s.Require().NoError(err)
	s.NotEqual(*id, *recreated)
}
