package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// KafkaQueue is a placeholder for future Kafka backend implementation.
type KafkaQueue struct{}

// NewKafkaQueue creates placeholder Kafka queue implementation.
func NewKafkaQueue() *KafkaQueue {
	return &KafkaQueue{}
}

// Enqueue is not implemented for Kafka backend yet.
func (q *KafkaQueue) Enqueue(
	ctx context.Context,
	taskName string,
	payload JSONPayload,
	availableAt time.Time,
	idemKey string,
) (*int64, error) {
	return nil, ErrNotImplemented
}

// Claim is not implemented for Kafka backend yet.
func (q *KafkaQueue) Claim(ctx context.Context, tasks []string, limit int) ([]Claimed, error) {
	return nil, ErrNotImplemented
}

// StartHeartbeat marks lease as lost for non-implemented backend.
func (q *KafkaQueue) StartHeartbeat(ctx context.Context, taskId int64, leaseToken uuid.UUID, lost chan struct{}) {
	select {
	case lost <- struct{}{}:
	default:
	}
}

// Ack is not implemented for Kafka backend yet.
func (q *KafkaQueue) Ack(ctx context.Context, taskId int64, leaseToken uuid.UUID) (bool, error) {
	return false, ErrNotImplemented
}

// Nack is not implemented for Kafka backend yet.
func (q *KafkaQueue) Nack(
	ctx context.Context,
	taskId int64,
	leaseToken uuid.UUID,
	errText string,
	delay time.Duration,
) (bool, error) {
	return false, ErrNotImplemented
}

// MoveToDLQ is not implemented for Kafka backend yet.
func (q *KafkaQueue) MoveToDLQ(ctx context.Context, taskId int64, leaseToken uuid.UUID, errText string) (bool, error) {
	return false, ErrNotImplemented
}
