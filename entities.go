package schedulor

import (
	"encoding/json"
	queue2 "schedulor/queue"
	"time"

	"github.com/google/uuid"
)

type LqTask struct {
	TaskID       int             `json:"task_id"`
	TaskName     string          `json:"task_name"`
	ProcessAfter time.Time       `json:"process_after"`
	LeaderID     *string         `json:"leader_id"`
	CreatedAt    time.Time       `json:"created_at"`
	Data         json.RawMessage `json:"data"`
	Meta         json.RawMessage `json:"meta"`
}

type LqSchedule struct {
	Id          uuid.UUID          `json:"id"`
	TaskName    string             `json:"task_name"`
	Cron        string             `json:"cron"`
	Payload     queue2.JSONPayload `json:"payload"`
	IsActive    bool               `json:"is_active"`
	NextRun     *time.Time         `json:"next_run"`
	LastRun     *time.Time         `json:"last_run"`
	TaskID      *int64             `json:"task_id"`
	Updated     time.Time          `json:"updated"`
	Description *string            `json:"description"`
}

type LqScheduleLeader struct {
	Key        string    `json:"key"`
	LeaderID   string    `json:"leader_id"`
	ValidUntil time.Time `json:"valid_until"`
}
