package execution

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Store persists executions, pipeline runs, leases, results, and transition events.
type Store interface {
	CreateExecution(context.Context, CreateExecution) (Execution, error)
	GetExecution(context.Context, uuid.UUID) (Execution, error)
	ListExecutions(context.Context, ListFilter) ([]Execution, error)
	ListRunExecutions(context.Context, uuid.UUID) ([]Execution, error)
	Claim(context.Context, string, []string, int, time.Duration) ([]Execution, error)
	ReapExpired(context.Context) ([]uuid.UUID, error)
	Heartbeat(context.Context, uuid.UUID, uuid.UUID, time.Duration) error
	Succeed(context.Context, uuid.UUID, uuid.UUID, json.RawMessage) error
	Retry(context.Context, uuid.UUID, uuid.UUID, string, time.Time) error
	Fail(context.Context, uuid.UUID, uuid.UUID, string) error
	CancelExecution(context.Context, uuid.UUID, string) error
	RestartExecution(context.Context, uuid.UUID, time.Time) (Execution, error)
	Events(context.Context, uuid.UUID) ([]Event, error)

	CreatePipelineRun(context.Context, CreatePipelineRun) (PipelineRun, error)
	GetPipelineRun(context.Context, uuid.UUID) (PipelineRun, error)
	ListPipelineRuns(context.Context, []RunStatus) ([]PipelineRun, error)
	SetPipelineRunStatus(context.Context, uuid.UUID, RunStatus, string) error
}
