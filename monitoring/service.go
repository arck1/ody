// Package monitoring exposes read and control operations for task executions and pipelines.
package monitoring

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/arck1/ody/execution"
)

// TaskReader provides operational task inspection without exposing worker mutations.
type TaskReader interface {
	ListTasks(context.Context, execution.ListFilter) ([]execution.Execution, error)
	Task(context.Context, uuid.UUID) (TaskDetails, error)
}

// PipelineReader provides pipeline inspection including all node executions.
type PipelineReader interface {
	ListPipelines(context.Context, execution.RunFilter) ([]execution.PipelineRun, error)
	Pipeline(context.Context, uuid.UUID) (PipelineDetails, error)
}

// Controller provides safe operator actions.
type Controller interface {
	RestartTask(context.Context, uuid.UUID) (execution.Execution, error)
	CancelTask(context.Context, uuid.UUID, string) error
}

// API is the complete operational contract used by CLI and HTTP transports.
type API interface {
	TaskReader
	PipelineReader
	Controller
}

type TaskDetails struct {
	Execution execution.Execution `json:"execution"`
	Events    []execution.Event   `json:"events"`
}

type PipelineDetails struct {
	Run        execution.PipelineRun `json:"run"`
	Executions []execution.Execution `json:"executions"`
}

type Persistence interface {
	execution.ExecutionRepository
	execution.PipelineRepository
}

type Service struct {
	store             Persistence
	pipelineRestarter PipelineRestarter
}

type PipelineRestarter interface {
	RestartExecution(context.Context, uuid.UUID) (execution.Execution, error)
}

type Option func(*Service)

func WithPipelineRestarter(restarter PipelineRestarter) Option {
	return func(service *Service) { service.pipelineRestarter = restarter }
}

var _ API = (*Service)(nil)

func New(store Persistence, options ...Option) (*Service, error) {
	if store == nil {
		return nil, errors.New("monitoring store is nil")
	}
	service := &Service{store: store}
	for _, option := range options {
		option(service)
	}
	return service, nil
}

func (s *Service) ListTasks(ctx context.Context, filter execution.ListFilter) ([]execution.Execution, error) {
	return s.store.ListExecutions(ctx, filter)
}

func (s *Service) Task(ctx context.Context, id uuid.UUID) (TaskDetails, error) {
	item, err := s.store.GetExecution(ctx, id)
	if err != nil {
		return TaskDetails{}, err
	}
	events, err := s.store.Events(ctx, id)
	if err != nil {
		return TaskDetails{}, err
	}
	return TaskDetails{Execution: item, Events: events}, nil
}

func (s *Service) ListPipelines(ctx context.Context, filter execution.RunFilter) ([]execution.PipelineRun, error) {
	return s.store.ListPipelineRuns(ctx, filter)
}

func (s *Service) Pipeline(ctx context.Context, id uuid.UUID) (PipelineDetails, error) {
	run, err := s.store.GetPipelineRun(ctx, id)
	if err != nil {
		return PipelineDetails{}, err
	}
	items, err := s.store.ListRunExecutions(ctx, id)
	if err != nil {
		return PipelineDetails{}, err
	}
	return PipelineDetails{Run: run, Executions: items}, nil
}

func (s *Service) RestartTask(ctx context.Context, id uuid.UUID) (execution.Execution, error) {
	item, err := s.store.GetExecution(ctx, id)
	if err != nil {
		return execution.Execution{}, err
	}
	if item.PipelineRunID != nil {
		if s.pipelineRestarter == nil {
			return execution.Execution{}, execution.ErrPipelineExecution
		}
		return s.pipelineRestarter.RestartExecution(ctx, id)
	}
	return s.store.RestartExecution(ctx, id, time.Time{})
}

func (s *Service) CancelTask(ctx context.Context, id uuid.UUID, reason string) error {
	return s.store.CancelExecution(ctx, id, reason)
}
