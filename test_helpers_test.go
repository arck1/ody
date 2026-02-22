package schedulor

import (
	"context"
	"schedulor/queue"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

type testQueueBackend struct{}

func (q *testQueueBackend) Enqueue(
	ctx context.Context,
	taskName string,
	payload queue.JSONPayload,
	availableAt time.Time,
	idemKey string,
) (*int64, error) {
	id := int64(1)
	return &id, nil
}

func (q *testQueueBackend) Claim(ctx context.Context, tasks []string, limit int) ([]queue.Claimed, error) {
	return nil, nil
}

func (q *testQueueBackend) StartHeartbeat(ctx context.Context, taskId int64, leaseToken uuid.UUID, lost chan struct{}) {
}

func (q *testQueueBackend) Ack(ctx context.Context, taskId int64, leaseToken uuid.UUID) (bool, error) {
	return true, nil
}

func (q *testQueueBackend) Nack(
	ctx context.Context,
	taskId int64,
	leaseToken uuid.UUID,
	errText string,
	delay time.Duration,
) (bool, error) {
	return true, nil
}

func (q *testQueueBackend) MoveToDLQ(ctx context.Context, taskId int64) (bool, error) {
	return true, nil
}
