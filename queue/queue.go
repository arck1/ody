package queue

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
)

// TasksQueue defines producer-side queue operations.
type TasksQueue interface {
	// Enqueue adds a task to backend queue with optional idempotency key.
	Enqueue(
		ctx context.Context,
		taskName string,
		payload JSONPayload,
		availableAt time.Time,
		idemKey string,
	) (*int64, error)
}

// WorkerQueue defines consumer-side queue operations.
type WorkerQueue interface {
	// Claim reserves ready tasks for processing.
	Claim(ctx context.Context, tasks []string, limit int) ([]Claimed, error)
	// StartHeartbeat extends task lease while processing continues.
	StartHeartbeat(ctx context.Context, taskId int64, leaseToken uuid.UUID, lost chan struct{})
	// Ack confirms successful processing and removes task from queue.
	Ack(ctx context.Context, taskId int64, leaseToken uuid.UUID) (bool, error)
	// Nack returns failed task back to queue with delay.
	Nack(ctx context.Context, taskId int64, leaseToken uuid.UUID, errText string, delay time.Duration) (bool, error)
	// MoveToDLQ atomically moves a task owned by leaseToken to the dead-letter queue.
	MoveToDLQ(ctx context.Context, taskId int64, leaseToken uuid.UUID, errText string) (bool, error)
}

// QueueBackend combines producer and worker queue capabilities.
type QueueBackend interface {
	TasksQueue
	WorkerQueue
}

type DbConnector interface {
	// GetConnect returns SQL connection used by queue backend.
	GetConnect(ctx context.Context) (*sql.DB, error)
}

// PostgresQueueOptions controls runtime behavior of Postgres queue backend.
type PostgresQueueOptions struct {
	// TaskMaxAttempts limits retries before DLQ move.
	TaskMaxAttempts int
	// TaskVisibility sets lease duration per claimed task.
	TaskVisibility time.Duration
}

// Claimed contains task row data reserved for processing.
type Claimed struct {
	// TaskID is claimed task identifier.
	TaskID int64 `json:"task_id"`
	// TaskName is claimed logical task key.
	TaskName string `json:"task_name"`
	// Payload is JSON payload to pass into executor.
	Payload JSONPayload `json:"payload"`
	// LeaseToken identifies current claim owner.
	LeaseToken uuid.UUID `json:"lease_token"`
	// ReservedUntil is lease expiration timestamp.
	ReservedUntil time.Time `json:"reserved_until"`
	// Attempts is current processing attempt.
	Attempts int `json:"attempts"`
	// MaxAttempts is retry limit configured for task.
	MaxAttempts int `json:"max_attempts"`
}
