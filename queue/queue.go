package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type TasksQueue interface {
	Enqueue(
		ctx context.Context,
		taskName string,
		payload datatypes.JSONType[map[string]any],
		availableAt time.Time,
		idemKey string,
	) (*int64, error)
}

type WorkerQueue interface {
	Claim(ctx context.Context, tasks []string, limit int) ([]Claimed, error)
	StartHeartbeat(ctx context.Context, taskId int64, leaseToken uuid.UUID, lost chan struct{})
	Ack(ctx context.Context, taskId int64, leaseToken uuid.UUID) (bool, error)
	Nack(ctx context.Context, taskId int64, leaseToken uuid.UUID, errText string, delay time.Duration) (bool, error)
	MoveToDLQ(ctx context.Context, taskId int64) (bool, error)
}

type QueueBackend interface {
	TasksQueue
	WorkerQueue
}

type DbConnector interface {
	GetConnect(ctx context.Context) (*gorm.DB, error)
}

type PostgresQueueOptions struct {
	TaskMaxAttempts int
	TaskVisibility  time.Duration
}

type Claimed struct {
	TaskID        int64                              `json:"task_id"        gorm:"column:task_id"`
	TaskName      string                             `json:"task_name"      gorm:"column:task_name"`
	Payload       datatypes.JSONType[map[string]any] `                      gorm:"column:payload;type:jsonb"`
	LeaseToken    uuid.UUID                          `json:"lease_token"    gorm:"column:lease_token"`
	ReservedUntil time.Time                          `json:"reserved_until" gorm:"column:reserved_until"`
	Attempts      int                                `json:"attempts"       gorm:"column:attempts"`
	MaxAttempts   int                                `json:"max_attempts"   gorm:"column:max_attempts"`
}
