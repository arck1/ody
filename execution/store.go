package execution

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Queue owns delivery, leases, retries, and terminal worker transitions.
// MemoryStore, postgres.Store, and redis.Store all implement this contract.
type Queue interface {
	Claim(context.Context, string, []TaskKey, int, time.Duration) ([]Execution, error)
	ReapExpired(context.Context) ([]uuid.UUID, error)
	Heartbeat(context.Context, uuid.UUID, uuid.UUID, time.Duration) error
	Succeed(context.Context, uuid.UUID, uuid.UUID, json.RawMessage) error
	Retry(context.Context, uuid.UUID, uuid.UUID, string, time.Time) error
	Fail(context.Context, uuid.UUID, uuid.UUID, string) error
}

// ExecutionRepository persists task content, history, and operator actions.
type ExecutionRepository interface {
	CreateExecution(context.Context, CreateExecution) (Execution, error)
	GetExecution(context.Context, uuid.UUID) (Execution, error)
	ListExecutions(context.Context, ListFilter) ([]Execution, error)
	ListRunExecutions(context.Context, uuid.UUID) ([]Execution, error)
	CancelExecution(context.Context, uuid.UUID, string) error
	RestartExecution(context.Context, uuid.UUID, time.Time) (Execution, error)
	Events(context.Context, uuid.UUID) ([]Event, error)
}

// PipelineRepository persists pipeline runs and their status.
type PipelineRepository interface {
	CreatePipelineRun(context.Context, CreatePipelineRun) (PipelineRun, error)
	GetPipelineRun(context.Context, uuid.UUID) (PipelineRun, error)
	ListPipelineRuns(context.Context, []RunStatus) ([]PipelineRun, error)
	SetPipelineRunStatus(context.Context, uuid.UUID, RunStatus, string) error
	RestartPipelineSubgraph(context.Context, RestartSubgraph) (Execution, error)
	ReleaseExecution(context.Context, uuid.UUID, json.RawMessage, time.Time) error
}

// Store is the complete persistence contract used by task, worker, pipeline, and monitoring.
type Store interface {
	Queue
	ExecutionRepository
	PipelineRepository
}
