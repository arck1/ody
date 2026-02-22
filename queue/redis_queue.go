package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type RedisQueue struct{}

func NewRedisQueue() *RedisQueue {
	return &RedisQueue{}
}

func (q *RedisQueue) Enqueue(
	ctx context.Context,
	taskName string,
	payload JSONPayload,
	availableAt time.Time,
	idemKey string,
) (*int64, error) {
	return nil, ErrNotImplemented
}

func (q *RedisQueue) Claim(ctx context.Context, tasks []string, limit int) ([]Claimed, error) {
	return nil, ErrNotImplemented
}

func (q *RedisQueue) StartHeartbeat(ctx context.Context, taskId int64, leaseToken uuid.UUID, lost chan struct{}) {
	select {
	case lost <- struct{}{}:
	default:
	}
}

func (q *RedisQueue) Ack(ctx context.Context, taskId int64, leaseToken uuid.UUID) (bool, error) {
	return false, ErrNotImplemented
}

func (q *RedisQueue) Nack(
	ctx context.Context,
	taskId int64,
	leaseToken uuid.UUID,
	errText string,
	delay time.Duration,
) (bool, error) {
	return false, ErrNotImplemented
}

func (q *RedisQueue) MoveToDLQ(ctx context.Context, taskId int64) (bool, error) {
	return false, ErrNotImplemented
}
