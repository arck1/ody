package schedulor

import (
	"encoding/json"
	"time"

	"schedulor/queue"

	"github.com/google/uuid"
)

type LqTask struct {
	// TaskID is a primary key in lq_tasks.
	TaskID int `json:"task_id"`
	// TaskName identifies executor handler.
	TaskName string `json:"task_name"`
	// ProcessAfter defines earliest processing time.
	ProcessAfter time.Time `json:"process_after"`
	// LeaderID stores node id that reserved task.
	LeaderID *string `json:"leader_id"`
	// CreatedAt is task creation timestamp.
	CreatedAt time.Time `json:"created_at"`
	// Data contains task payload.
	Data json.RawMessage `json:"data"`
	// Meta contains optional task metadata.
	Meta json.RawMessage `json:"meta"`
}

// LqSchedule represents persisted cron schedule definition.
type LqSchedule struct {
	// Id is schedule UUID.
	Id uuid.UUID `json:"id"`
	// TaskName is executor task key to enqueue.
	TaskName string `json:"task_name"`
	// Cron stores cron expression.
	Cron string `json:"cron"`
	// Payload is JSON payload enqueued on run.
	Payload queue.JSONPayload `json:"payload"`
	// IsActive controls whether schedule should run.
	IsActive bool `json:"is_active"`
	// NextRun is projected next run timestamp.
	NextRun *time.Time `json:"next_run"`
	// LastRun is timestamp of last successful enqueue.
	LastRun *time.Time `json:"last_run"`
	// TaskID references last created runtime task id.
	TaskID *int64 `json:"task_id"`
	// Updated tracks row changes used for in-memory job refresh.
	Updated time.Time `json:"updated"`
	// Description stores optional human-readable details.
	Description *string `json:"description"`
}

// LqScheduleLeader stores distributed leader lease state.
type LqScheduleLeader struct {
	// Key identifies leader election group.
	Key string `json:"key"`
	// LeaderID is current owner id.
	LeaderID string `json:"leader_id"`
	// ValidUntil is lease expiration time.
	ValidUntil time.Time `json:"valid_until"`
}
