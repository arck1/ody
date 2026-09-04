package queue

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
)

type RedisQueueSuite struct {
	suite.Suite
	server *miniredis.Miniredis
	client *redis.Client
	queue  *RedisQueue
	now    time.Time
}

func TestRedisQueueSuite(t *testing.T) {
	suite.Run(t, new(RedisQueueSuite))
}

func (s *RedisQueueSuite) SetupTest() {
	s.server = miniredis.RunT(s.T())
	s.client = redis.NewClient(&redis.Options{Addr: s.server.Addr()})
	s.now = time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC)
	s.server.SetTime(s.now)
	var err error
	s.queue, err = NewRedisQueue(s.client, RedisQueueOptions{
		Prefix:          "test:{queue}:",
		TaskMaxAttempts: 3,
		TaskVisibility:  time.Minute,
	})
	s.Require().NoError(err)
	s.queue.now = func() time.Time { return s.now }
}

func (s *RedisQueueSuite) TearDownTest() {
	s.Require().NoError(s.client.Close())
	s.server.Close()
}

func (s *RedisQueueSuite) TestEnqueueIsIdempotentAndAckIsLeaseSafe() {
	ctx := context.Background()
	payload := JSONPayload(`{"value":42}`)
	first, err := s.queue.Enqueue(ctx, "email", payload, s.now, "email:42")
	s.Require().NoError(err)
	duplicate, err := s.queue.Enqueue(ctx, "email", payload, s.now, "email:42")
	s.Require().NoError(err)
	s.Equal(*first, *duplicate)

	otherTasks, err := s.queue.Claim(ctx, []string{"other"}, 1)
	s.Require().NoError(err)
	s.Empty(otherTasks)
	claimed, err := s.queue.Claim(ctx, []string{"email"}, 1)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal(*first, claimed[0].TaskID)
	s.Equal("email", claimed[0].TaskName)
	s.Equal(payload, claimed[0].Payload)
	s.Equal(1, claimed[0].Attempts)
	s.Equal(3, claimed[0].MaxAttempts)
	s.Equal(s.now.Add(time.Minute), claimed[0].ReservedUntil)

	removed, err := s.queue.Ack(ctx, *first, uuid.New())
	s.Require().NoError(err)
	s.False(removed)
	removed, err = s.queue.Ack(ctx, *first, claimed[0].LeaseToken)
	s.Require().NoError(err)
	s.True(removed)

	recreated, err := s.queue.Enqueue(ctx, "email", payload, s.now, "email:42")
	s.Require().NoError(err)
	s.NotEqual(*first, *recreated)
}

func (s *RedisQueueSuite) TestNackDelaysRedeliveryAndRejectsStaleLease() {
	ctx := context.Background()
	id, err := s.queue.Enqueue(ctx, "sync", JSONPayload(`{"page":1}`), s.now, "")
	s.Require().NoError(err)
	first, err := s.queue.Claim(ctx, []string{"sync"}, 1)
	s.Require().NoError(err)
	s.Require().Len(first, 1)

	updated, err := s.queue.Nack(ctx, *id, uuid.New(), "stale", time.Minute)
	s.Require().NoError(err)
	s.False(updated)
	updated, err = s.queue.Nack(ctx, *id, first[0].LeaseToken, "temporary", time.Minute)
	s.Require().NoError(err)
	s.True(updated)
	claimed, err := s.queue.Claim(ctx, []string{"sync"}, 1)
	s.Require().NoError(err)
	s.Empty(claimed)

	s.advance(time.Minute)
	second, err := s.queue.Claim(ctx, []string{"sync"}, 1)
	s.Require().NoError(err)
	s.Require().Len(second, 1)
	s.Equal(2, second[0].Attempts)
	s.NotEqual(first[0].LeaseToken, second[0].LeaseToken)
	s.advance(20 * time.Second)
	heartbeat, err := s.queue.Heartbeat(ctx, *id, first[0].LeaseToken)
	s.Require().NoError(err)
	s.False(heartbeat)
	heartbeat, err = s.queue.Heartbeat(ctx, *id, second[0].LeaseToken)
	s.Require().NoError(err)
	s.True(heartbeat)
	reservedUntil := s.server.HGet(s.queue.taskKey(*id), "reserved_until")
	s.Equal(stringID(s.now.Add(time.Minute).UnixMilli()), reservedUntil)
}

func (s *RedisQueueSuite) TestClaimReturnsOldestTasksAcrossNames() {
	ctx := context.Background()
	first, err := s.queue.Enqueue(ctx, "first", JSONPayload(`{}`), s.now.Add(2*time.Second), "")
	s.Require().NoError(err)
	second, err := s.queue.Enqueue(ctx, "second", JSONPayload(`{}`), s.now.Add(time.Second), "")
	s.Require().NoError(err)
	s.advance(3 * time.Second)

	claimed, err := s.queue.Claim(ctx, []string{"first", "second", "first"}, 2)
	s.Require().NoError(err)
	s.Require().Len(claimed, 2)
	s.Equal(*second, claimed[0].TaskID)
	s.Equal(*first, claimed[1].TaskID)
}

func (s *RedisQueueSuite) TestMoveToDLQPreservesFailureData() {
	ctx := context.Background()
	id, err := s.queue.Enqueue(ctx, "billing", JSONPayload(`{"invoice":"A"}`), s.now, "invoice:A")
	s.Require().NoError(err)
	claimed, err := s.queue.Claim(ctx, []string{"billing"}, 1)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	moved, err := s.queue.MoveToDLQ(ctx, *id, uuid.New(), "stale")
	s.Require().NoError(err)
	s.False(moved)
	moved, err = s.queue.MoveToDLQ(ctx, *id, claimed[0].LeaseToken, "payment rejected")
	s.Require().NoError(err)
	s.True(moved)
	s.False(s.server.Exists(s.queue.taskKey(*id)))
	s.Equal("payment rejected", s.server.HGet(s.queue.dlqKeyPrefix()+stringID(*id), "last_error"))
	s.True(s.server.Exists(s.queue.dlqIndexKey()))
}

func (s *RedisQueueSuite) TestExpiredFinalLeaseIsMovedToDLQ() {
	ctx := context.Background()
	queue, err := NewRedisQueue(s.client, RedisQueueOptions{
		Prefix:          "exhausted:{queue}:",
		TaskMaxAttempts: 1,
		TaskVisibility:  time.Minute,
	})
	s.Require().NoError(err)
	queue.now = func() time.Time { return s.now }
	id, err := queue.Enqueue(ctx, "fragile", JSONPayload(`{}`), s.now, "")
	s.Require().NoError(err)
	claimed, err := queue.Claim(ctx, []string{"fragile"}, 1)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	s.advance(time.Minute)
	reclaimed, err := queue.Claim(ctx, []string{"fragile"}, 1)
	s.Require().NoError(err)
	s.Empty(reclaimed)
	s.False(s.server.Exists(queue.taskKey(*id)))
	s.Equal("lease expired after maximum attempts", s.server.HGet(queue.dlqKeyPrefix()+stringID(*id), "last_error"))
}

func stringID(id int64) string {
	return strconv.FormatInt(id, 10)
}

func (s *RedisQueueSuite) advance(duration time.Duration) {
	s.now = s.now.Add(duration)
	s.server.SetTime(s.now)
}
