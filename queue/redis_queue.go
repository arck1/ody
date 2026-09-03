package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// RedisQueue is a placeholder for future Redis backend implementation.
type RedisQueue struct{}

// NewRedisQueue creates placeholder Redis queue implementation.
func NewRedisQueue() *RedisQueue {
	return &RedisQueue{}
}

// Enqueue is not implemented for Redis backend yet.
func (q *RedisQueue) Enqueue(
	ctx context.Context,
	taskName string,
	payload JSONPayload,
	availableAt time.Time,
	idemKey string,
) (*int64, error) {
	return nil, ErrNotImplemented
}

// Claim is not implemented for Redis backend yet.
func (q *RedisQueue) Claim(ctx context.Context, tasks []string, limit int) ([]Claimed, error) {
	return nil, ErrNotImplemented
}

// StartHeartbeat marks lease as lost for non-implemented backend.
func (q *RedisQueue) StartHeartbeat(ctx context.Context, taskId int64, leaseToken uuid.UUID, lost chan struct{}) {
	select {
	case lost <- struct{}{}:
	default:
	}
}

// Ack is not implemented for Redis backend yet.
func (q *RedisQueue) Ack(ctx context.Context, taskId int64, leaseToken uuid.UUID) (bool, error) {
	return false, ErrNotImplemented
}

// Nack is not implemented for Redis backend yet.
func (q *RedisQueue) Nack(
	ctx context.Context,
	taskId int64,
	leaseToken uuid.UUID,
	errText string,
	delay time.Duration,
) (bool, error) {
	return false, ErrNotImplemented
}

// MoveToDLQ is not implemented for Redis backend yet.
func (q *RedisQueue) MoveToDLQ(ctx context.Context, taskId int64, leaseToken uuid.UUID, errText string) (bool, error) {
	return false, ErrNotImplemented
}
