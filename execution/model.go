// Package execution defines persistent task execution state and storage contracts.
package execution

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending   Status = "pending"
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
	ErrNotFound  = errors.New("execution not found")
	ErrLeaseLost = errors.New("execution lease lost")
	ErrActive    = errors.New("execution is active")
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
