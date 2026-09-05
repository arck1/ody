package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MemoryStore is a concurrency-safe reference Store for tests and local processes.
type MemoryStore struct {
	mu         sync.Mutex
	executions map[uuid.UUID]Execution
	runs       map[uuid.UUID]PipelineRun
	events     map[uuid.UUID][]Event
	idempotent map[string]uuid.UUID
	nodes      map[string]uuid.UUID
	now        func() time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		executions: make(map[uuid.UUID]Execution),
		runs:       make(map[uuid.UUID]PipelineRun),
		events:     make(map[uuid.UUID][]Event),
		idempotent: make(map[string]uuid.UUID),
		nodes:      make(map[string]uuid.UUID),
		now:        func() time.Time { return time.Now().UTC() },
	}
}

// CreateExecution provides the same idempotency and pipeline-node uniqueness behavior as durable
// stores, but scopes it to this process.
func (s *MemoryStore) CreateExecution(_ context.Context, request CreateExecution) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.TaskName == "" {
		return Execution{}, fmt.Errorf("task name is empty")
	}
	if request.IdempotencyKey != "" {
		key := request.TaskName + "\x00" + request.IdempotencyKey
		if id, ok := s.idempotent[key]; ok {
			return cloneExecution(s.executions[id]), nil
		}
	}
	if request.PipelineRunID != nil && request.NodeKey != "" {
		key := request.PipelineRunID.String() + "\x00" + request.NodeKey
		if id, ok := s.nodes[key]; ok {
			return cloneExecution(s.executions[id]), nil
		}
	}
	now := s.now()
	if request.AvailableAt.IsZero() {
		request.AvailableAt = now
	}
	if request.MaxAttempts <= 0 {
		request.MaxAttempts = 1
	}
	item := Execution{
		ID:             uuid.New(),
		TaskName:       request.TaskName,
		TaskVersion:    request.TaskVersion,
		Input:          cloneJSON(request.Input),
		Status:         StatusPending,
		MaxAttempts:    request.MaxAttempts,
		AvailableAt:    request.AvailableAt,
		IdempotencyKey: request.IdempotencyKey,
		PipelineRunID:  cloneUUID(request.PipelineRunID),
		NodeKey:        request.NodeKey,
		CreatedAt:      now,
	}
	s.executions[item.ID] = item
	if request.IdempotencyKey != "" {
		s.idempotent[request.TaskName+"\x00"+request.IdempotencyKey] = item.ID
	}
	if request.PipelineRunID != nil && request.NodeKey != "" {
		s.nodes[request.PipelineRunID.String()+"\x00"+request.NodeKey] = item.ID
	}
	s.appendEvent(item, EventCreated, "")
	return cloneExecution(item), nil
}

func (s *MemoryStore) GetExecution(_ context.Context, id uuid.UUID) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.executions[id]
	if !ok {
		return Execution{}, ErrNotFound
	}
	return cloneExecution(item), nil
}

func (s *MemoryStore) ListExecutions(_ context.Context, filter ListFilter) ([]Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]Execution, 0)
	for _, item := range s.executions {
		if filter.TaskName != "" && item.TaskName != filter.TaskName {
			continue
		}
		if filter.Status != "" && item.Status != filter.Status {
			continue
		}
		if filter.PipelineRunID != nil && (item.PipelineRunID == nil || *item.PipelineRunID != *filter.PipelineRunID) {
			continue
		}
		items = append(items, cloneExecution(item))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	if filter.Limit > 0 && len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	return items, nil
}

func (s *MemoryStore) ListRunExecutions(_ context.Context, runID uuid.UUID) ([]Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]Execution, 0)
	for _, item := range s.executions {
		if item.PipelineRunID != nil && *item.PipelineRunID == runID {
			items = append(items, cloneExecution(item))
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	return items, nil
}

func (s *MemoryStore) Claim(_ context.Context, owner string, names []string, limit int, lease time.Duration) ([]Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 1
	}
	accepted := make(map[string]struct{}, len(names))
	for _, name := range names {
		accepted[name] = struct{}{}
	}
	now := s.now()
	candidates := make([]Execution, 0)
	for _, item := range s.executions {
		if _, ok := accepted[item.TaskName]; !ok || item.AvailableAt.After(now) {
			continue
		}
		claimable := item.Status == StatusPending || item.Status == StatusRetry ||
			(item.Status == StatusRunning && !item.LeaseUntil.After(now))
		if !claimable {
			continue
		}
		if item.Attempt >= item.MaxAttempts {
			continue
		}
		candidates = append(candidates, item)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].AvailableAt.Equal(candidates[j].AvailableAt) {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return candidates[i].AvailableAt.Before(candidates[j].AvailableAt)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	claimed := make([]Execution, 0, len(candidates))
	for _, candidate := range candidates {
		candidate.Status = StatusRunning
		candidate.Attempt++
		candidate.LeaseOwner = owner
		candidate.LeaseToken = uuid.New()
		candidate.LeaseUntil = now.Add(lease)
		if candidate.StartedAt == nil {
			candidate.StartedAt = new(now)
		}
		s.executions[candidate.ID] = candidate
		s.appendEvent(candidate, EventStarted, "")
		claimed = append(claimed, cloneExecution(candidate))
	}
	return claimed, nil
}

// ReapExpired fails exhausted expired deliveries and returns affected pipeline runs.
func (s *MemoryStore) ReapExpired(_ context.Context) ([]uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	runs := map[uuid.UUID]struct{}{}
	for id, item := range s.executions {
		if item.Status != StatusRunning || item.LeaseUntil.After(now) || item.Attempt < item.MaxAttempts {
			continue
		}
		item.Status, item.LastError, item.FinishedAt = StatusFailed, "lease expired after maximum attempts", new(now)
		item.LeaseToken, item.LeaseOwner, item.LeaseUntil = uuid.Nil, "", time.Time{}
		s.executions[id] = item
		s.appendEvent(item, EventFailed, item.LastError)
		if item.PipelineRunID != nil {
			runs[*item.PipelineRunID] = struct{}{}
		}
	}
	ids := make([]uuid.UUID, 0, len(runs))
	for id := range runs {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *MemoryStore) Heartbeat(_ context.Context, id, token uuid.UUID, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, err := s.owned(id, token)
	if err != nil {
		return err
	}
	item.LeaseUntil = s.now().Add(lease)
	s.executions[id] = item
	return nil
}

func (s *MemoryStore) Succeed(_ context.Context, id, token uuid.UUID, output json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, err := s.owned(id, token)
	if err != nil {
		return err
	}
	now := s.now()
	item.Status, item.Output, item.FinishedAt = StatusSucceeded, cloneJSON(output), new(now)
	item.LeaseToken, item.LeaseOwner, item.LeaseUntil = uuid.Nil, "", time.Time{}
	s.executions[id] = item
	s.appendEvent(item, EventSucceeded, "")
	return nil
}

func (s *MemoryStore) Retry(_ context.Context, id, token uuid.UUID, errorText string, availableAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, err := s.owned(id, token)
	if err != nil {
		return err
	}
	item.LastError = errorText
	item.LeaseToken, item.LeaseOwner, item.LeaseUntil = uuid.Nil, "", time.Time{}
	if item.Attempt >= item.MaxAttempts {
		now := s.now()
		item.Status, item.FinishedAt = StatusFailed, new(now)
		s.executions[id] = item
		s.appendEvent(item, EventFailed, errorText)
		return nil
	}
	item.Status, item.AvailableAt = StatusRetry, availableAt
	s.executions[id] = item
	s.appendEvent(item, EventRetried, errorText)
	return nil
}

func (s *MemoryStore) Fail(_ context.Context, id, token uuid.UUID, errorText string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, err := s.owned(id, token)
	if err != nil {
		return err
	}
	now := s.now()
	item.Status, item.LastError, item.FinishedAt = StatusFailed, errorText, new(now)
	item.LeaseToken, item.LeaseOwner, item.LeaseUntil = uuid.Nil, "", time.Time{}
	s.executions[id] = item
	s.appendEvent(item, EventFailed, errorText)
	return nil
}

func (s *MemoryStore) CancelExecution(_ context.Context, id uuid.UUID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.executions[id]
	if !ok {
		return ErrNotFound
	}
	now := s.now()
	item.Status, item.LastError, item.FinishedAt = StatusCancelled, reason, new(now)
	item.LeaseToken, item.LeaseOwner, item.LeaseUntil = uuid.Nil, "", time.Time{}
	s.executions[id] = item
	s.appendEvent(item, EventCancelled, reason)
	return nil
}

func (s *MemoryStore) RestartExecution(_ context.Context, id uuid.UUID, availableAt time.Time) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.executions[id]
	if !ok {
		return Execution{}, ErrNotFound
	}
	if item.Status == StatusPending || item.Status == StatusRetry || item.Status == StatusRunning {
		return Execution{}, ErrActive
	}
	if availableAt.IsZero() {
		availableAt = s.now()
	}
	item.Status, item.Attempt, item.AvailableAt = StatusPending, 0, availableAt
	item.Output, item.LastError, item.StartedAt, item.FinishedAt = nil, "", nil, nil
	item.LeaseToken, item.LeaseOwner, item.LeaseUntil = uuid.Nil, "", time.Time{}
	s.executions[id] = item
	s.appendEvent(item, EventRestarted, "")
	if item.PipelineRunID != nil {
		run, exists := s.runs[*item.PipelineRunID]
		if exists {
			run.Status, run.Error, run.FinishedAt, run.UpdatedAt = RunRunning, "", nil, s.now()
			s.runs[run.ID] = run
		}
	}
	return cloneExecution(item), nil
}

func (s *MemoryStore) Events(_ context.Context, id uuid.UUID) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.executions[id]; !ok {
		return nil, ErrNotFound
	}
	return append([]Event(nil), s.events[id]...), nil
}

func (s *MemoryStore) CreatePipelineRun(_ context.Context, request CreatePipelineRun) (PipelineRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	run := PipelineRun{
		ID:              uuid.New(),
		PipelineName:    request.PipelineName,
		PipelineVersion: request.PipelineVersion,
		Input:           cloneJSON(request.Input),
		Status:          RunPending,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	s.runs[run.ID] = run
	return cloneRun(run), nil
}

func (s *MemoryStore) GetPipelineRun(_ context.Context, id uuid.UUID) (PipelineRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[id]
	if !ok {
		return PipelineRun{}, ErrNotFound
	}
	return cloneRun(run), nil
}

func (s *MemoryStore) ListPipelineRuns(_ context.Context, statuses []RunStatus) ([]PipelineRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	accepted := map[RunStatus]struct{}{}
	for _, status := range statuses {
		accepted[status] = struct{}{}
	}
	items := make([]PipelineRun, 0)
	for _, run := range s.runs {
		if len(accepted) > 0 {
			if _, ok := accepted[run.Status]; !ok {
				continue
			}
		}
		items = append(items, cloneRun(run))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	return items, nil
}

func (s *MemoryStore) SetPipelineRunStatus(_ context.Context, id uuid.UUID, status RunStatus, errorText string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[id]
	if !ok {
		return ErrNotFound
	}
	now := s.now()
	run.Status, run.Error, run.UpdatedAt = status, errorText, now
	if status == RunSucceeded || status == RunFailed || status == RunCancelled {
		run.FinishedAt = new(now)
	}
	s.runs[id] = run
	return nil
}

func (s *MemoryStore) owned(id, token uuid.UUID) (Execution, error) {
	item, ok := s.executions[id]
	if !ok {
		return Execution{}, ErrNotFound
	}
	if item.Status != StatusRunning || token == uuid.Nil || item.LeaseToken != token {
		return Execution{}, ErrLeaseLost
	}
	return item, nil
}

func (s *MemoryStore) appendEvent(item Execution, eventType EventType, errorText string) {
	event := Event{
		ID: uuid.New(), ExecutionID: item.ID, Type: eventType,
		Attempt: item.Attempt, Error: errorText, CreatedAt: s.now(),
	}
	s.events[item.ID] = append(s.events[item.ID], event)
}

func cloneExecution(item Execution) Execution {
	item.Input, item.Output = cloneJSON(item.Input), cloneJSON(item.Output)
	item.PipelineRunID = cloneUUID(item.PipelineRunID)
	return item
}

func cloneRun(run PipelineRun) PipelineRun            { run.Input = cloneJSON(run.Input); return run }
func cloneJSON(value json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), value...) }
func cloneUUID(value *uuid.UUID) *uuid.UUID {
	if value == nil {
		return nil
	}
	return new(*value)
}
