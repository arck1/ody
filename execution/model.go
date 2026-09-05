// Package execution defines persistent task execution state and storage contracts.
package execution

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TaskKey is the complete routing identity of a task handler. Workers advertise exact keys so an
// older deployment cannot claim executions created for a newer task version.
type TaskKey struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

func (k TaskKey) String() string { return fmt.Sprintf("%s@v%d", k.Name, k.Version) }

type Status string

const (
	StatusPending Status = "pending"
	// StatusBlocked is used only for an existing pipeline descendant waiting for restarted
	// predecessors. Queue implementations must never claim blocked executions.
	StatusBlocked   Status = "blocked"
	StatusRunning   Status = "running"
	StatusRetry     Status = "retry"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

var (
	ErrNotFound          = errors.New("execution not found")
	ErrLeaseLost         = errors.New("execution lease lost")
	ErrActive            = errors.New("execution is active")
	ErrPipelineExecution = errors.New("pipeline execution requires pipeline coordinator")
)

// Execution is one durable invocation of a task definition.
type Execution struct {
	ID             uuid.UUID       `json:"id"`
	TaskName       string          `json:"task_name"`
	TaskVersion    int             `json:"task_version"`
	Input          json.RawMessage `json:"input"`
	Output         json.RawMessage `json:"output,omitempty"`
	Status         Status          `json:"status"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	AvailableAt    time.Time       `json:"available_at"`
	LeaseOwner     string          `json:"lease_owner,omitempty"`
	LeaseToken     uuid.UUID       `json:"lease_token,omitempty"`
	LeaseUntil     time.Time       `json:"lease_until,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	PipelineRunID  *uuid.UUID      `json:"pipeline_run_id,omitempty"`
	NodeKey        string          `json:"node_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

type CreateExecution struct {
	TaskName       string
	TaskVersion    int
	Input          json.RawMessage
	MaxAttempts    int
	AvailableAt    time.Time
	IdempotencyKey string
	PipelineRunID  *uuid.UUID
	NodeKey        string
}

type ListFilter struct {
	TaskName      string
	Status        Status
	PipelineRunID *uuid.UUID
	Limit         int
	Before        *Cursor
}

// Cursor identifies the last item of a descending page. Pass it as Before to continue without
// offset scans; ID makes equal timestamps deterministic.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type RunFilter struct {
	Statuses []RunStatus
	Limit    int
	Before   *Cursor
}

// PipelineRun tracks one durable pipeline invocation.
type PipelineRun struct {
	ID              uuid.UUID       `json:"id"`
	PipelineName    string          `json:"pipeline_name"`
	PipelineVersion int             `json:"pipeline_version"`
	Input           json.RawMessage `json:"input"`
	Status          RunStatus       `json:"status"`
	Error           string          `json:"error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
}

type CreatePipelineRun struct {
	PipelineName    string
	PipelineVersion int
	Input           json.RawMessage
}

// RestartSubgraph describes a pipeline restart compiled from the registered DAG. RootNodeKey is
// made pending immediately; descendants are reset to blocked until Engine releases them.
type RestartSubgraph struct {
	RunID          uuid.UUID
	RootNodeKey    string
	DescendantKeys []string
	AvailableAt    time.Time
}

type EventType string

const (
	EventCreated   EventType = "created"
	EventStarted   EventType = "started"
	EventRetried   EventType = "retried"
	EventSucceeded EventType = "succeeded"
	EventFailed    EventType = "failed"
	EventCancelled EventType = "cancelled"
	EventRestarted EventType = "restarted"
)

// Event is an append-only execution transition record.
type Event struct {
	ID          uuid.UUID `json:"id"`
	ExecutionID uuid.UUID `json:"execution_id"`
	Type        EventType `json:"type"`
	Attempt     int       `json:"attempt"`
	Error       string    `json:"error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type ExecutionCount struct {
	TaskName string
	Status   Status
	Count    int64
}

type PipelineCount struct {
	PipelineName string
	Status       RunStatus
	Count        int64
}
