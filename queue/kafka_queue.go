package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

type KafkaQueue struct{}

func NewKafkaQueue() *KafkaQueue {
	return &KafkaQueue{}
}

func (q *KafkaQueue) Enqueue(
	ctx context.Context,
	taskName string,
	payload datatypes.JSONType[map[string]any],
	availableAt time.Time,
	idemKey string,
) (*int64, error) {
	return nil, ErrNotImplemented
}

func (q *KafkaQueue) Claim(ctx context.Context, tasks []string, limit int) ([]Claimed, error) {
	return nil, ErrNotImplemented
}

func (q *KafkaQueue) StartHeartbeat(ctx context.Context, taskId int64, leaseToken uuid.UUID, lost chan struct{}) {
	select {
	case lost <- struct{}{}:
	default:
	}
}

func (q *KafkaQueue) Ack(ctx context.Context, taskId int64, leaseToken uuid.UUID) (bool, error) {
	return false, ErrNotImplemented
}

func (q *KafkaQueue) Nack(
	ctx context.Context,
	taskId int64,
	leaseToken uuid.UUID,
	errText string,
	delay time.Duration,
) (bool, error) {
	return false, ErrNotImplemented
}

func (q *KafkaQueue) MoveToDLQ(ctx context.Context, taskId int64) (bool, error) {
	return false, ErrNotImplemented
}
