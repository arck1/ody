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
	mu            sync.Mutex
	executions    map[uuid.UUID]Execution
	runs          map[uuid.UUID]PipelineRun
	events        map[uuid.UUID][]Event
	idempotent    map[string]uuid.UUID
	nodes         map[string]uuid.UUID
	runIdempotent map[string]uuid.UUID
	now           func() time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		executions:    make(map[uuid.UUID]Execution),
		runs:          make(map[uuid.UUID]PipelineRun),
		events:        make(map[uuid.UUID][]Event),
		idempotent:    make(map[string]uuid.UUID),
		nodes:         make(map[string]uuid.UUID),
		runIdempotent: make(map[string]uuid.UUID),
		now:           func() time.Time { return time.Now().UTC() },
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
		if filter.Before != nil && !beforeCursor(item.CreatedAt, item.ID, *filter.Before) {
			continue
		}
		items = append(items, cloneExecution(item))
	}
	sort.Slice(items, func(i, j int) bool { return newer(items[i].CreatedAt, items[i].ID, items[j].CreatedAt, items[j].ID) })
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

func (s *MemoryStore) Claim(_ context.Context, owner string, keys []TaskKey, limit int, lease time.Duration) ([]Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 1
	}
	accepted := make(map[TaskKey]struct{}, len(keys))
	for _, key := range keys {
		accepted[key] = struct{}{}
	}
	now := s.now()
	candidates := make([]Execution, 0)
	for _, item := range s.executions {
		if _, ok := accepted[TaskKey{Name: item.TaskName, Version: item.TaskVersion}]; !ok || item.AvailableAt.After(now) {
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

func (s *MemoryStore) Retry(_ context.Context, id, token uuid.UUID, errorText string, delay time.Duration) error {
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
	item.Status, item.AvailableAt = StatusRetry, s.now().Add(delay)
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
	if item.Status == StatusSucceeded || item.Status == StatusFailed || item.Status == StatusCancelled {
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
	if item.PipelineRunID != nil {
		return Execution{}, ErrPipelineExecution
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
	return cloneExecution(item), nil
}

func (s *MemoryStore) RestartPipelineSubgraph(_ context.Context, request RestartSubgraph) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[request.RunID]
	if !ok {
		return Execution{}, ErrNotFound
	}
	descendants := make(map[string]struct{}, len(request.DescendantKeys))
	for _, key := range request.DescendantKeys {
		descendants[key] = struct{}{}
	}
	var root Execution
	foundRoot := false
	for _, item := range s.executions {
		if item.PipelineRunID == nil || *item.PipelineRunID != request.RunID {
			continue
		}
		if item.NodeKey != request.RootNodeKey {
			if _, affected := descendants[item.NodeKey]; !affected {
				continue
			}
		}
		if item.Status == StatusPending || item.Status == StatusRetry || item.Status == StatusRunning {
			return Execution{}, ErrActive
		}
		if item.NodeKey == request.RootNodeKey {
			root, foundRoot = item, true
		}
	}
	if !foundRoot {
		return Execution{}, ErrNotFound
	}
	now := s.now()
	if request.AvailableAt.IsZero() {
		request.AvailableAt = now
	}
	for id, item := range s.executions {
		if item.PipelineRunID == nil || *item.PipelineRunID != request.RunID {
			continue
		}
		status := Status("")
		switch {
		case item.NodeKey == request.RootNodeKey:
			status = StatusPending
		case hasKey(descendants, item.NodeKey):
			status = StatusBlocked
		default:
			continue
		}
		item.Status, item.Attempt, item.AvailableAt = status, 0, request.AvailableAt
		item.Output, item.LastError, item.StartedAt, item.FinishedAt = nil, "", nil, nil
		item.LeaseToken, item.LeaseOwner, item.LeaseUntil = uuid.Nil, "", time.Time{}
		s.executions[id] = item
		s.appendEvent(item, EventRestarted, "")
		if id == root.ID {
			root = item
		}
	}
	run.Status, run.Error, run.FinishedAt, run.UpdatedAt, run.Revision = RunRunning, "", nil, now, run.Revision+1
	s.runs[run.ID] = run
	return cloneExecution(root), nil
}

func (s *MemoryStore) ReleaseExecution(_ context.Context, id uuid.UUID, input json.RawMessage, availableAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.executions[id]
	if !ok {
		return ErrNotFound
	}
	if item.Status != StatusBlocked {
		return ErrActive
	}
	if availableAt.IsZero() {
		availableAt = s.now()
	}
	item.Status, item.Input, item.AvailableAt = StatusPending, cloneJSON(input), availableAt
	s.executions[id] = item
	return nil
}

func hasKey(values map[string]struct{}, key string) bool {
	_, ok := values[key]
	return ok
}

func newer(leftTime time.Time, leftID uuid.UUID, rightTime time.Time, rightID uuid.UUID) bool {
	if leftTime.Equal(rightTime) {
		return leftID.String() > rightID.String()
	}
	return leftTime.After(rightTime)
}

func beforeCursor(createdAt time.Time, id uuid.UUID, cursor Cursor) bool {
	return createdAt.Before(cursor.CreatedAt) || createdAt.Equal(cursor.CreatedAt) && id.String() < cursor.ID.String()
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
	if request.IdempotencyKey != "" {
		key := request.PipelineName + "\x00" + request.IdempotencyKey
		if id, ok := s.runIdempotent[key]; ok {
			return cloneRun(s.runs[id]), nil
		}
	}
	now := s.now()
	run := PipelineRun{
		ID:              uuid.New(),
		PipelineName:    request.PipelineName,
		PipelineVersion: request.PipelineVersion,
		Input:           cloneJSON(request.Input),
		Status:          RunPending,
		IdempotencyKey:  request.IdempotencyKey,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	s.runs[run.ID] = run
	if request.IdempotencyKey != "" {
		s.runIdempotent[request.PipelineName+"\x00"+request.IdempotencyKey] = run.ID
	}
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

func (s *MemoryStore) ListPipelineRuns(_ context.Context, filter RunFilter) ([]PipelineRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	accepted := map[RunStatus]struct{}{}
	for _, status := range filter.Statuses {
		accepted[status] = struct{}{}
	}
	items := make([]PipelineRun, 0)
	for _, run := range s.runs {
		if len(accepted) > 0 {
			if _, ok := accepted[run.Status]; !ok {
				continue
			}
		}
		if filter.Before != nil && !beforeCursor(run.CreatedAt, run.ID, *filter.Before) {
			continue
		}
		items = append(items, cloneRun(run))
	}
	sort.Slice(items, func(i, j int) bool { return newer(items[i].CreatedAt, items[i].ID, items[j].CreatedAt, items[j].ID) })
	if filter.Limit > 0 && len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	return items, nil
}

func (s *MemoryStore) SetPipelineRunStatus(_ context.Context, id uuid.UUID, expectedRevision uint64, status RunStatus, errorText string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[id]
	if !ok {
		return ErrNotFound
	}
	if run.Revision != expectedRevision {
		return ErrConflict
	}
	now := s.now()
	run.Status, run.Error, run.UpdatedAt, run.Revision = status, errorText, now, run.Revision+1
	if status == RunSucceeded || status == RunFailed || status == RunCancelled {
		run.FinishedAt = new(now)
	}
	s.runs[id] = run
	return nil
}

func (s *MemoryStore) ExecutionCounts(_ context.Context) ([]ExecutionCount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := make(map[[2]string]int64)
	for _, item := range s.executions {
		counts[[2]string{item.TaskName, string(item.Status)}]++
	}
	result := make([]ExecutionCount, 0, len(counts))
	for key, count := range counts {
		result = append(result, ExecutionCount{TaskName: key[0], Status: Status(key[1]), Count: count})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].TaskName == result[j].TaskName {
			return result[i].Status < result[j].Status
		}
		return result[i].TaskName < result[j].TaskName
	})
	return result, nil
}

func (s *MemoryStore) PipelineCounts(_ context.Context) ([]PipelineCount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := make(map[[2]string]int64)
	for _, run := range s.runs {
		counts[[2]string{run.PipelineName, string(run.Status)}]++
	}
	result := make([]PipelineCount, 0, len(counts))
	for key, count := range counts {
		result = append(result, PipelineCount{PipelineName: key[0], Status: RunStatus(key[1]), Count: count})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].PipelineName == result[j].PipelineName {
			return result[i].Status < result[j].Status
		}
		return result[i].PipelineName < result[j].PipelineName
	})
	return result, nil
}

func (s *MemoryStore) Purge(_ context.Context, before time.Time, limit int) (PurgeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 1000
	}
	type candidate struct {
		id       uuid.UUID
		finished time.Time
		pipeline bool
	}
	candidates := make([]candidate, 0)
	for id, run := range s.runs {
		if run.FinishedAt != nil && run.FinishedAt.Before(before) {
			candidates = append(candidates, candidate{id: id, finished: *run.FinishedAt, pipeline: true})
		}
	}
	for id, item := range s.executions {
		if item.PipelineRunID == nil && item.FinishedAt != nil && item.FinishedAt.Before(before) {
			candidates = append(candidates, candidate{id: id, finished: *item.FinishedAt})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].finished.Before(candidates[j].finished) })
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	result := PurgeResult{}
	for _, candidate := range candidates {
		if candidate.pipeline {
			run := s.runs[candidate.id]
			for id, item := range s.executions {
				if item.PipelineRunID != nil && *item.PipelineRunID == candidate.id {
					s.deleteExecution(id, item)
					result.Executions++
				}
			}
			delete(s.runs, candidate.id)
			if run.IdempotencyKey != "" {
				delete(s.runIdempotent, run.PipelineName+"\x00"+run.IdempotencyKey)
			}
			result.PipelineRuns++
			continue
		}
		item := s.executions[candidate.id]
		s.deleteExecution(candidate.id, item)
		result.Executions++
	}
	return result, nil
}

func (s *MemoryStore) deleteExecution(id uuid.UUID, item Execution) {
	delete(s.executions, id)
	delete(s.events, id)
	if item.IdempotencyKey != "" {
		delete(s.idempotent, item.TaskName+"\x00"+item.IdempotencyKey)
	}
	if item.PipelineRunID != nil {
		delete(s.nodes, item.PipelineRunID.String()+"\x00"+item.NodeKey)
	}
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
