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
)

// Execution is one durable invocation of a task definition.
type Execution struct {
	ID             uuid.UUID
	TaskName       string
	TaskVersion    int
	Input          json.RawMessage
	Output         json.RawMessage
	Status         Status
	Attempt        int
	MaxAttempts    int
	AvailableAt    time.Time
	LeaseOwner     string
	LeaseToken     uuid.UUID
	LeaseUntil     time.Time
	LastError      string
	IdempotencyKey string
	PipelineRunID  *uuid.UUID
	NodeKey        string
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
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
	ID              uuid.UUID
	PipelineName    string
	PipelineVersion int
	Input           json.RawMessage
	Status          RunStatus
	Error           string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	FinishedAt      *time.Time
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
)

// Event is an append-only execution transition record.
type Event struct {
	ID          uuid.UUID
	ExecutionID uuid.UUID
	Type        EventType
	Attempt     int
	Error       string
	CreatedAt   time.Time
}
