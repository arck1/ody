package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	redislib "github.com/redis/go-redis/v9"

	"github.com/arck1/ody/execution"
)

const defaultPrefix = "ody:{execution}:"

// Options configures Redis key names.
type Options struct {
	// Prefix isolates applications and environments. Keep a common {...} hash tag for Redis Cluster.
	Prefix string
}

// Store persists the complete execution model and uses sorted sets as its delivery queue.
// It never closes the client; client ownership remains with the application.
type Store struct {
	client redislib.UniversalClient
	prefix string
}

var _ execution.Store = (*Store)(nil)

func New(client redislib.UniversalClient, options Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("redis execution store client is nil")
	}
	if options.Prefix == "" {
		options.Prefix = defaultPrefix
	}
	return &Store{client: client, prefix: options.Prefix}, nil
}

// CreateExecution atomically creates the record, its queue/index entries, and the created event.
// Existing idempotency or pipeline-node references return the original execution.
func (s *Store) CreateExecution(ctx context.Context, request execution.CreateExecution) (execution.Execution, error) {
	if request.TaskName == "" {
		return execution.Execution{}, errors.New("task name is empty")
	}
	now, err := s.now(ctx)
	if err != nil {
		return execution.Execution{}, err
	}
	if request.AvailableAt.IsZero() {
		request.AvailableAt = now
	}
	if request.MaxAttempts <= 0 {
		request.MaxAttempts = 1
	}
	item := execution.Execution{
		ID: uuid.New(), TaskName: request.TaskName, TaskVersion: request.TaskVersion,
		Input: cloneJSON(request.Input), Status: execution.StatusPending, MaxAttempts: request.MaxAttempts,
		AvailableAt: request.AvailableAt.UTC(), IdempotencyKey: request.IdempotencyKey,
		PipelineRunID: cloneUUID(request.PipelineRunID), NodeKey: request.NodeKey, CreatedAt: now,
	}
	refs := make([]string, 0, 2)
	if item.IdempotencyKey != "" {
		refs = append(refs, s.idempotencyKey(item.TaskName, item.IdempotencyKey))
	}
	if item.PipelineRunID != nil && item.NodeKey != "" {
		refs = append(refs, s.nodeKey(*item.PipelineRunID, item.NodeKey))
	}
	var existing uuid.UUID
	err = s.watch(ctx, refs, func(tx *redislib.Tx) error {
		for _, key := range refs {
			value, getErr := tx.Get(ctx, key).Result()
			if getErr == nil {
				existing, getErr = uuid.Parse(value)
				if getErr != nil {
					return fmt.Errorf("invalid redis execution reference %q: %w", key, getErr)
				}
				return errExisting
			}
			if !errors.Is(getErr, redislib.Nil) {
				return getErr
			}
		}
		encoded, encodeErr := json.Marshal(item)
		if encodeErr != nil {
			return encodeErr
		}
		event, encodeErr := encodeEvent(item, execution.EventCreated, "", now)
		if encodeErr != nil {
			return encodeErr
		}
		_, encodeErr = tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
			pipe.Set(ctx, s.executionKey(item.ID), encoded, 0)
			pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(item.Status)), 1)
			pipe.ZAdd(ctx, s.executionsKey(), redislib.Z{Score: score(item.CreatedAt), Member: item.ID.String()})
			pipe.ZAdd(ctx, s.taskIndexKey(item.TaskName), redislib.Z{Score: score(item.CreatedAt), Member: item.ID.String()})
			pipe.ZAdd(ctx, s.statusKey(item.Status), redislib.Z{Score: score(item.CreatedAt), Member: item.ID.String()})
			pipe.ZAdd(ctx, s.queueKey(item.TaskName), redislib.Z{Score: score(item.AvailableAt), Member: item.ID.String()})
			pipe.RPush(ctx, s.eventsKey(item.ID), event)
			for _, key := range refs {
				pipe.Set(ctx, key, item.ID.String(), 0)
			}
			if item.PipelineRunID != nil {
				pipe.ZAdd(ctx, s.runExecutionsKey(*item.PipelineRunID), redislib.Z{Score: score(item.CreatedAt), Member: item.ID.String()})
			}
			return nil
		})
		return encodeErr
	})
	if errors.Is(err, errExisting) {
		return s.GetExecution(ctx, existing)
	}
	return item, err
}

// GetExecution reads the source-of-truth execution record rather than reconstructing it from indexes.
func (s *Store) GetExecution(ctx context.Context, id uuid.UUID) (execution.Execution, error) {
	value, err := s.client.Get(ctx, s.executionKey(id)).Bytes()
	if errors.Is(err, redislib.Nil) {
		return execution.Execution{}, execution.ErrNotFound
	}
	if err != nil {
		return execution.Execution{}, err
	}
	var item execution.Execution
	if err = json.Unmarshal(value, &item); err != nil {
		return execution.Execution{}, fmt.Errorf("decode redis execution %s: %w", id, err)
	}
	return item, nil
}

// ListExecutions selects the narrowest available index and applies any remaining filters to records.
func (s *Store) ListExecutions(ctx context.Context, filter execution.ListFilter) ([]execution.Execution, error) {
	ids, err := s.client.ZRevRange(ctx, s.executionIndexFor(filter), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	items := make([]execution.Execution, 0, len(ids))
	for _, value := range ids {
		id, parseErr := uuid.Parse(value)
		if parseErr != nil {
			return nil, parseErr
		}
		item, getErr := s.GetExecution(ctx, id)
		if getErr != nil {
			return nil, getErr
		}
		if !matchesExecutionFilter(item, filter) {
			continue
		}
		items = append(items, item)
		if filter.Limit > 0 && len(items) == filter.Limit {
			break
		}
	}
	return items, nil
}

// executionIndexFor avoids a full scan whenever the filter supplies an indexed dimension.
func (s *Store) executionIndexFor(filter execution.ListFilter) string {
	switch {
	case filter.PipelineRunID != nil:
		return s.runExecutionsKey(*filter.PipelineRunID)
	case filter.TaskName != "":
		return s.taskIndexKey(filter.TaskName)
	case filter.Status != "":
		return s.statusKey(filter.Status)
	default:
		return s.executionsKey()
	}
}

func matchesExecutionFilter(item execution.Execution, filter execution.ListFilter) bool {
	if filter.TaskName != "" && item.TaskName != filter.TaskName {
		return false
	}
	if filter.Status != "" && item.Status != filter.Status {
		return false
	}
	if filter.Before != nil && !before(item.CreatedAt, item.ID, *filter.Before) {
		return false
	}
	return filter.PipelineRunID == nil || item.PipelineRunID != nil && *item.PipelineRunID == *filter.PipelineRunID
}

func before(createdAt time.Time, id uuid.UUID, cursor execution.Cursor) bool {
	return createdAt.Before(cursor.CreatedAt) || createdAt.Equal(cursor.CreatedAt) && id.String() < cursor.ID.String()
}

// ListRunExecutions returns pipeline nodes in creation order.
func (s *Store) ListRunExecutions(ctx context.Context, runID uuid.UUID) ([]execution.Execution, error) {
	ids, err := s.client.ZRange(ctx, s.runExecutionsKey(runID), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	items := make([]execution.Execution, 0, len(ids))
	for _, value := range ids {
		id, parseErr := uuid.Parse(value)
		if parseErr != nil {
			return nil, parseErr
		}
		item, getErr := s.GetExecution(ctx, id)
		if getErr != nil {
			return nil, getErr
		}
		items = append(items, item)
	}
	return items, nil
}

// Claim acquires up to limit due executions for owner.
//
// Candidates are discovered through per-task queue indexes, then claimed one at a time with WATCH.
// Claiming one record per transaction keeps contention local: workers racing for the same task may
// lose that candidate without rolling back other records they already claimed.
func (s *Store) Claim(ctx context.Context, owner string, keys []execution.TaskKey, limit int, lease time.Duration) ([]execution.Execution, error) {
	if limit <= 0 {
		limit = 1
	}
	now, err := s.now(ctx)
	if err != nil {
		return nil, err
	}
	accepted := make(map[execution.TaskKey]struct{}, len(keys))
	names := make([]string, 0, len(keys))
	seenNames := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		accepted[key] = struct{}{}
		if _, exists := seenNames[key.Name]; !exists {
			seenNames[key.Name] = struct{}{}
			names = append(names, key.Name)
		}
	}
	ordered, err := s.loadDueCandidates(ctx, names, limit, now)
	if err != nil {
		return nil, err
	}
	claimed := make([]execution.Execution, 0, limit)
	for _, candidate := range ordered {
		if len(claimed) == limit {
			break
		}
		result, won, claimErr := s.claimCandidate(ctx, candidate.ID, owner, accepted, lease, now)
		if claimErr != nil {
			return nil, claimErr
		}
		if !won {
			continue
		}
		claimed = append(claimed, result)
	}
	return claimed, nil
}

// claimCandidate converts discovery into ownership. A false result is an expected race: the
// candidate disappeared, became ineligible, or was claimed by another worker first.
func (s *Store) claimCandidate(ctx context.Context, id uuid.UUID, owner string, accepted map[execution.TaskKey]struct{}, lease time.Duration, now time.Time) (execution.Execution, bool, error) {
	var result execution.Execution
	err := s.watch(ctx, []string{s.executionKey(id)}, func(tx *redislib.Tx) error {
		item, getErr := s.getExecutionTx(ctx, tx, id)
		if getErr != nil {
			return getErr
		}
		_, nameAccepted := accepted[execution.TaskKey{Name: item.TaskName, Version: item.TaskVersion}]
		claimable := item.Status == execution.StatusPending || item.Status == execution.StatusRetry ||
			(item.Status == execution.StatusRunning && !item.LeaseUntil.After(now))
		if !nameAccepted || !claimable || item.AvailableAt.After(now) || item.Attempt >= item.MaxAttempts {
			return errUnavailable
		}

		previous := item.Status
		item.Status, item.Attempt, item.LeaseOwner = execution.StatusRunning, item.Attempt+1, owner
		item.LeaseToken, item.LeaseUntil = uuid.New(), now.Add(lease)
		if item.StartedAt == nil {
			item.StartedAt = new(now)
		}
		if getErr = s.saveTransition(ctx, tx, item, previous, execution.EventStarted, "", now, true); getErr != nil {
			return getErr
		}
		result = item
		return nil
	})
	if errors.Is(err, errUnavailable) || errors.Is(err, execution.ErrNotFound) {
		return execution.Execution{}, false, nil
	}
	return result, err == nil, err
}

// loadDueCandidates merges the due heads of all requested task queues. Reading more than limit
// leaves room for candidates lost to another worker between discovery and WATCH.
func (s *Store) loadDueCandidates(ctx context.Context, names []string, limit int, now time.Time) ([]execution.Execution, error) {
	candidates := make(map[uuid.UUID]execution.Execution)
	for _, name := range names {
		values, err := s.client.ZRangeByScore(ctx, s.queueKey(name), &redislib.ZRangeBy{
			Min: "-inf", Max: fmt.Sprint(score(now)), Count: int64(max(limit*8, 128)),
		}).Result()
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			id, parseErr := uuid.Parse(value)
			if parseErr != nil {
				return nil, parseErr
			}
			item, getErr := s.GetExecution(ctx, id)
			if errors.Is(getErr, execution.ErrNotFound) {
				// A missing source record means the derived queue entry is stale and safe to discard.
				_ = s.client.ZRem(ctx, s.queueKey(name), value).Err()
				continue
			}
			if getErr != nil {
				return nil, getErr
			}
			candidates[id] = item
		}
	}

	ordered := make([]execution.Execution, 0, len(candidates))
	for _, item := range candidates {
		ordered = append(ordered, item)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].AvailableAt.Equal(ordered[j].AvailableAt) {
			return ordered[i].CreatedAt.Before(ordered[j].CreatedAt)
		}
		return ordered[i].AvailableAt.Before(ordered[j].AvailableAt)
	})
	return ordered, nil
}

// ReapExpired terminally fails final attempts whose worker lease expired.
// Non-final expired attempts stay in the task queue and can be claimed by another worker.
func (s *Store) ReapExpired(ctx context.Context) ([]uuid.UUID, error) {
	now, err := s.now(ctx)
	if err != nil {
		return nil, err
	}
	values, err := s.client.ZRangeByScore(ctx, s.leasesKey(), &redislib.ZRangeBy{Min: "-inf", Max: fmt.Sprint(score(now))}).Result()
	if err != nil {
		return nil, err
	}
	runs := make(map[uuid.UUID]struct{})
	for _, value := range values {
		id, parseErr := uuid.Parse(value)
		if parseErr != nil {
			return nil, parseErr
		}
		runID, reapErr := s.reapCandidate(ctx, id, now)
		if reapErr != nil {
			return nil, reapErr
		}
		if runID != nil {
			runs[*runID] = struct{}{}
		}
	}
	result := make([]uuid.UUID, 0, len(runs))
	for id := range runs {
		result = append(result, id)
	}
	return result, nil
}

// reapCandidate removes a stale lease index for recoverable attempts. Only the final expired
// attempt becomes failed; its pipeline ID is returned so the worker can advance the run.
func (s *Store) reapCandidate(ctx context.Context, id uuid.UUID, now time.Time) (*uuid.UUID, error) {
	var runID *uuid.UUID
	err := s.watch(ctx, []string{s.executionKey(id)}, func(tx *redislib.Tx) error {
		item, getErr := s.getExecutionTx(ctx, tx, id)
		if errors.Is(getErr, execution.ErrNotFound) {
			return s.removeLeaseIndex(ctx, tx, id)
		}
		if getErr != nil {
			return getErr
		}
		if item.Status != execution.StatusRunning {
			return s.removeLeaseIndex(ctx, tx, id)
		}
		if item.LeaseUntil.After(now) {
			return errUnavailable
		}
		if item.Attempt < item.MaxAttempts {
			return s.removeLeaseIndex(ctx, tx, id)
		}

		previous := item.Status
		item.Status, item.LastError, item.FinishedAt = execution.StatusFailed, "lease expired after maximum attempts", new(now)
		clearLease(&item)
		runID = cloneUUID(item.PipelineRunID)
		return s.saveTransition(ctx, tx, item, previous, execution.EventFailed, item.LastError, now, false)
	})
	if errors.Is(err, errUnavailable) {
		return nil, nil
	}
	return runID, err
}

func (s *Store) removeLeaseIndex(ctx context.Context, tx *redislib.Tx, id uuid.UUID) error {
	_, err := tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
		pipe.ZRem(ctx, s.leasesKey(), id.String())
		return nil
	})
	return err
}

// Heartbeat extends an owned execution lease and moves both deadline indexes atomically.
func (s *Store) Heartbeat(ctx context.Context, id, token uuid.UUID, lease time.Duration) error {
	now, err := s.now(ctx)
	if err != nil {
		return err
	}
	return s.watch(ctx, []string{s.executionKey(id)}, func(tx *redislib.Tx) error {
		item, getErr := s.getExecutionTx(ctx, tx, id)
		if getErr != nil {
			return getErr
		}
		if item.Status != execution.StatusRunning || token == uuid.Nil || item.LeaseToken != token {
			return execution.ErrLeaseLost
		}
		item.LeaseUntil = now.Add(lease)
		encoded, getErr := json.Marshal(item)
		if getErr != nil {
			return getErr
		}
		_, getErr = tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
			pipe.Set(ctx, s.executionKey(id), encoded, 0)
			pipe.ZAdd(ctx, s.queueKey(item.TaskName), redislib.Z{Score: score(item.LeaseUntil), Member: id.String()})
			pipe.ZAdd(ctx, s.leasesKey(), redislib.Z{Score: score(item.LeaseUntil), Member: id.String()})
			return nil
		})
		return getErr
	})
}

// Succeed records the task output, releases its lease, and removes it from delivery indexes.
func (s *Store) Succeed(ctx context.Context, id, token uuid.UUID, output json.RawMessage) error {
	return s.transition(ctx, id, token, func(item *execution.Execution, now time.Time) (execution.EventType, string, bool) {
		item.Status, item.Output, item.FinishedAt = execution.StatusSucceeded, cloneJSON(output), new(now)
		clearLease(item)
		return execution.EventSucceeded, "", false
	})
}

// Retry reschedules an owned execution, or fails it when its attempt budget is exhausted.
func (s *Store) Retry(ctx context.Context, id, token uuid.UUID, errorText string, delay time.Duration) error {
	return s.transition(ctx, id, token, func(item *execution.Execution, now time.Time) (execution.EventType, string, bool) {
		item.LastError = errorText
		clearLease(item)
		if item.Attempt >= item.MaxAttempts {
			item.Status, item.FinishedAt = execution.StatusFailed, new(now)
			return execution.EventFailed, errorText, false
		}
		item.Status, item.AvailableAt = execution.StatusRetry, now.Add(delay)
		return execution.EventRetried, errorText, true
	})
}

// Fail terminally records a permanent task error.
func (s *Store) Fail(ctx context.Context, id, token uuid.UUID, errorText string) error {
	return s.transition(ctx, id, token, func(item *execution.Execution, now time.Time) (execution.EventType, string, bool) {
		item.Status, item.LastError, item.FinishedAt = execution.StatusFailed, errorText, new(now)
		clearLease(item)
		return execution.EventFailed, errorText, false
	})
}

// transition applies a lease-token-guarded mutation to a running execution.
func (s *Store) transition(ctx context.Context, id, token uuid.UUID, mutate func(*execution.Execution, time.Time) (execution.EventType, string, bool)) error {
	now, err := s.now(ctx)
	if err != nil {
		return err
	}
	return s.watch(ctx, []string{s.executionKey(id)}, func(tx *redislib.Tx) error {
		item, getErr := s.getExecutionTx(ctx, tx, id)
		if getErr != nil {
			return getErr
		}
		if item.Status != execution.StatusRunning || token == uuid.Nil || item.LeaseToken != token {
			return execution.ErrLeaseLost
		}
		previous := item.Status
		eventType, errorText, queued := mutate(&item, now)
		return s.saveTransition(ctx, tx, item, previous, eventType, errorText, now, queued)
	})
}

// CancelExecution invalidates the lease and prevents any pending delivery.
func (s *Store) CancelExecution(ctx context.Context, id uuid.UUID, reason string) error {
	now, err := s.now(ctx)
	if err != nil {
		return err
	}
	return s.watch(ctx, []string{s.executionKey(id)}, func(tx *redislib.Tx) error {
		item, getErr := s.getExecutionTx(ctx, tx, id)
		if getErr != nil {
			return getErr
		}
		if item.Status == execution.StatusSucceeded || item.Status == execution.StatusFailed || item.Status == execution.StatusCancelled {
			return execution.ErrNotFound
		}
		previous := item.Status
		item.Status, item.LastError, item.FinishedAt = execution.StatusCancelled, reason, new(now)
		clearLease(&item)
		return s.saveTransition(ctx, tx, item, previous, execution.EventCancelled, reason, now, false)
	})
}

// RestartExecution reuses the execution identity and history while resetting mutable attempt state.
func (s *Store) RestartExecution(ctx context.Context, id uuid.UUID, availableAt time.Time) (execution.Execution, error) {
	now, err := s.now(ctx)
	if err != nil {
		return execution.Execution{}, err
	}
	if availableAt.IsZero() {
		availableAt = now
	}
	var result execution.Execution
	err = s.watch(ctx, []string{s.executionKey(id)}, func(tx *redislib.Tx) error {
		item, getErr := s.getExecutionTx(ctx, tx, id)
		if getErr != nil {
			return getErr
		}
		if item.Status == execution.StatusPending || item.Status == execution.StatusRetry || item.Status == execution.StatusRunning {
			return execution.ErrActive
		}
		if item.PipelineRunID != nil {
			return execution.ErrPipelineExecution
		}
		previous := item.Status
		item.Status, item.Attempt, item.AvailableAt = execution.StatusPending, 0, availableAt.UTC()
		item.Output, item.LastError, item.StartedAt, item.FinishedAt = nil, "", nil, nil
		clearLease(&item)
		encoded, getErr := json.Marshal(item)
		if getErr != nil {
			return getErr
		}
		event, getErr := encodeEvent(item, execution.EventRestarted, "", now)
		if getErr != nil {
			return getErr
		}
		_, getErr = tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
			pipe.Set(ctx, s.executionKey(id), encoded, 0)
			pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(previous)), -1)
			pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(item.Status)), 1)
			pipe.ZRem(ctx, s.statusKey(previous), id.String())
			pipe.ZAdd(ctx, s.statusKey(item.Status), redislib.Z{Score: score(item.CreatedAt), Member: id.String()})
			pipe.ZRem(ctx, s.leasesKey(), id.String())
			pipe.ZAdd(ctx, s.queueKey(item.TaskName), redislib.Z{Score: score(item.AvailableAt), Member: id.String()})
			pipe.RPush(ctx, s.eventsKey(id), event)
			return nil
		})
		result = item
		return getErr
	})
	return result, err
}

func (s *Store) RestartPipelineSubgraph(ctx context.Context, request execution.RestartSubgraph) (execution.Execution, error) {
	now, err := s.now(ctx)
	if err != nil {
		return execution.Execution{}, err
	}
	if request.AvailableAt.IsZero() {
		request.AvailableAt = now
	}
	nodeKeys := append([]string{request.RootNodeKey}, request.DescendantKeys...)
	ids := make([]uuid.UUID, 0, len(nodeKeys))
	watchKeys := []string{s.pipelineKey(request.RunID)}
	for _, nodeKey := range nodeKeys {
		value, getErr := s.client.Get(ctx, s.nodeKey(request.RunID, nodeKey)).Result()
		if errors.Is(getErr, redislib.Nil) {
			continue
		}
		if getErr != nil {
			return execution.Execution{}, getErr
		}
		id, parseErr := uuid.Parse(value)
		if parseErr != nil {
			return execution.Execution{}, parseErr
		}
		ids = append(ids, id)
		watchKeys = append(watchKeys, s.executionKey(id))
	}
	var root execution.Execution
	err = s.watch(ctx, watchKeys, func(tx *redislib.Tx) error {
		runValues, getErr := tx.HGetAll(ctx, s.pipelineKey(request.RunID)).Result()
		if getErr != nil {
			return getErr
		}
		if len(runValues) == 0 {
			return execution.ErrNotFound
		}
		run, getErr := decodeRun(runValues)
		if getErr != nil {
			return getErr
		}
		items := make([]execution.Execution, 0, len(ids))
		for _, id := range ids {
			item, itemErr := s.getExecutionTx(ctx, tx, id)
			if itemErr != nil {
				return itemErr
			}
			if item.Status == execution.StatusPending || item.Status == execution.StatusRetry || item.Status == execution.StatusRunning {
				return execution.ErrActive
			}
			items = append(items, item)
		}
		foundRoot := false
		previousRunStatus := run.Status
		run.Status, run.Error, run.UpdatedAt, run.FinishedAt, run.Revision = execution.RunRunning, "", now, nil, run.Revision+1
		_, getErr = tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
			for index := range items {
				item := &items[index]
				previous := item.Status
				if item.NodeKey == request.RootNodeKey {
					item.Status = execution.StatusPending
					foundRoot = true
				} else {
					item.Status = execution.StatusBlocked
				}
				item.Attempt, item.AvailableAt = 0, request.AvailableAt.UTC()
				item.Output, item.LastError, item.StartedAt, item.FinishedAt = nil, "", nil, nil
				clearLease(item)
				if item.NodeKey == request.RootNodeKey {
					root = *item
				}
				encoded, encodeErr := json.Marshal(item)
				if encodeErr != nil {
					return encodeErr
				}
				event, encodeErr := encodeEvent(*item, execution.EventRestarted, "", now)
				if encodeErr != nil {
					return encodeErr
				}
				pipe.Set(ctx, s.executionKey(item.ID), encoded, 0)
				if previous != item.Status {
					pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(previous)), -1)
					pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(item.Status)), 1)
				}
				pipe.ZRem(ctx, s.statusKey(previous), item.ID.String())
				pipe.ZAdd(ctx, s.statusKey(item.Status), redislib.Z{Score: score(item.CreatedAt), Member: item.ID.String()})
				pipe.ZRem(ctx, s.leasesKey(), item.ID.String())
				if item.Status == execution.StatusPending {
					pipe.ZAdd(ctx, s.queueKey(item.TaskName), redislib.Z{Score: score(item.AvailableAt), Member: item.ID.String()})
				} else {
					pipe.ZRem(ctx, s.queueKey(item.TaskName), item.ID.String())
				}
				pipe.RPush(ctx, s.eventsKey(item.ID), event)
			}
			if !foundRoot {
				return execution.ErrNotFound
			}
			pipe.HSet(ctx, s.pipelineKey(run.ID), encodeRun(run))
			if previousRunStatus != run.Status {
				pipe.HIncrBy(ctx, s.pipelineCountsKey(), countField(run.PipelineName, string(previousRunStatus)), -1)
				pipe.HIncrBy(ctx, s.pipelineCountsKey(), countField(run.PipelineName, string(run.Status)), 1)
			}
			pipe.ZRem(ctx, s.runStatusKey(previousRunStatus), run.ID.String())
			pipe.ZAdd(ctx, s.runStatusKey(run.Status), redislib.Z{Score: score(run.CreatedAt), Member: run.ID.String()})
			return nil
		})
		return getErr
	})
	return root, err
}

func (s *Store) ReleaseExecution(ctx context.Context, id uuid.UUID, input json.RawMessage, availableAt time.Time) error {
	now, err := s.now(ctx)
	if err != nil {
		return err
	}
	if availableAt.IsZero() {
		availableAt = now
	}
	return s.watch(ctx, []string{s.executionKey(id)}, func(tx *redislib.Tx) error {
		item, getErr := s.getExecutionTx(ctx, tx, id)
		if getErr != nil {
			return getErr
		}
		if item.Status != execution.StatusBlocked {
			return execution.ErrActive
		}
		item.Status, item.Input, item.AvailableAt = execution.StatusPending, cloneJSON(input), availableAt.UTC()
		encoded, getErr := json.Marshal(item)
		if getErr != nil {
			return getErr
		}
		_, getErr = tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
			pipe.Set(ctx, s.executionKey(id), encoded, 0)
			pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(execution.StatusBlocked)), -1)
			pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(execution.StatusPending)), 1)
			pipe.ZRem(ctx, s.statusKey(execution.StatusBlocked), id.String())
			pipe.ZAdd(ctx, s.statusKey(execution.StatusPending), redislib.Z{Score: score(item.CreatedAt), Member: id.String()})
			pipe.ZAdd(ctx, s.queueKey(item.TaskName), redislib.Z{Score: score(item.AvailableAt), Member: id.String()})
			return nil
		})
		return getErr
	})
}

// Events returns the append-only transition history in insertion order.
func (s *Store) Events(ctx context.Context, id uuid.UUID) ([]execution.Event, error) {
	if _, err := s.GetExecution(ctx, id); err != nil {
		return nil, err
	}
	values, err := s.client.LRange(ctx, s.eventsKey(id), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	items := make([]execution.Event, 0, len(values))
	for _, value := range values {
		var item execution.Event
		if err = json.Unmarshal([]byte(value), &item); err != nil {
			return nil, fmt.Errorf("decode redis event: %w", err)
		}
		items = append(items, item)
	}
	return items, nil
}

var (
	errExisting    = errors.New("redis execution reference already exists")
	errUnavailable = errors.New("redis execution is not claimable")
)

// saveTransition must be called inside WATCH. It keeps the record, queue, lease, status index, and
// event list consistent in one MULTI/EXEC transaction.
func (s *Store) saveTransition(ctx context.Context, tx *redislib.Tx, item execution.Execution, previous execution.Status, eventType execution.EventType, errorText string, now time.Time, keepInQueue bool) error {
	encoded, err := json.Marshal(item)
	if err != nil {
		return err
	}
	event, err := encodeEvent(item, eventType, errorText, now)
	if err != nil {
		return err
	}
	_, err = tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
		pipe.Set(ctx, s.executionKey(item.ID), encoded, 0)
		if previous != item.Status {
			pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(previous)), -1)
			pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(item.Status)), 1)
		}
		pipe.ZRem(ctx, s.statusKey(previous), item.ID.String())
		pipe.ZAdd(ctx, s.statusKey(item.Status), redislib.Z{Score: score(item.CreatedAt), Member: item.ID.String()})
		pipe.ZRem(ctx, s.leasesKey(), item.ID.String())
		if keepInQueue {
			queueAt := item.AvailableAt
			if item.Status == execution.StatusRunning {
				queueAt = item.LeaseUntil
				pipe.ZAdd(ctx, s.leasesKey(), redislib.Z{Score: score(item.LeaseUntil), Member: item.ID.String()})
			}
			pipe.ZAdd(ctx, s.queueKey(item.TaskName), redislib.Z{Score: score(queueAt), Member: item.ID.String()})
		} else {
			pipe.ZRem(ctx, s.queueKey(item.TaskName), item.ID.String())
		}
		pipe.RPush(ctx, s.eventsKey(item.ID), event)
		return nil
	})
	return err
}

func (s *Store) ExecutionCounts(ctx context.Context) ([]execution.ExecutionCount, error) {
	values, err := s.client.HGetAll(ctx, s.executionCountsKey()).Result()
	if err != nil {
		return nil, err
	}
	result := make([]execution.ExecutionCount, 0, len(values))
	for field, rawCount := range values {
		name, status, count, parseErr := parseCount(field, rawCount)
		if parseErr != nil {
			return nil, parseErr
		}
		if count > 0 {
			result = append(result, execution.ExecutionCount{TaskName: name, Status: execution.Status(status), Count: count})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].TaskName == result[j].TaskName {
			return result[i].Status < result[j].Status
		}
		return result[i].TaskName < result[j].TaskName
	})
	return result, nil
}

func (s *Store) Purge(ctx context.Context, before time.Time, limit int) (execution.PurgeResult, error) {
	if limit <= 0 {
		limit = 1000
	}
	type candidate struct {
		finished time.Time
		run      *execution.PipelineRun
		item     *execution.Execution
		nodes    []execution.Execution
	}
	candidates := make([]candidate, 0)
	runs, err := s.ListPipelineRuns(ctx, execution.RunFilter{Statuses: []execution.RunStatus{
		execution.RunSucceeded, execution.RunFailed, execution.RunCancelled,
	}})
	if err != nil {
		return execution.PurgeResult{}, err
	}
	for index := range runs {
		if runs[index].FinishedAt != nil && runs[index].FinishedAt.Before(before) {
			nodes, listErr := s.ListRunExecutions(ctx, runs[index].ID)
			if listErr != nil {
				return execution.PurgeResult{}, listErr
			}
			candidates = append(candidates, candidate{finished: *runs[index].FinishedAt, run: &runs[index], nodes: nodes})
		}
	}
	for _, status := range []execution.Status{execution.StatusSucceeded, execution.StatusFailed, execution.StatusCancelled} {
		items, listErr := s.ListExecutions(ctx, execution.ListFilter{Status: status})
		if listErr != nil {
			return execution.PurgeResult{}, listErr
		}
		for index := range items {
			if items[index].PipelineRunID == nil && items[index].FinishedAt != nil && items[index].FinishedAt.Before(before) {
				candidates = append(candidates, candidate{finished: *items[index].FinishedAt, item: &items[index]})
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].finished.Before(candidates[j].finished) })
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	result := execution.PurgeResult{}
	_, err = s.client.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
		for _, candidate := range candidates {
			if candidate.item != nil {
				s.purgeExecution(ctx, pipe, *candidate.item)
				result.Executions++
				continue
			}
			for _, item := range candidate.nodes {
				s.purgeExecution(ctx, pipe, item)
				result.Executions++
			}
			run := *candidate.run
			pipe.Del(ctx, s.pipelineKey(run.ID))
			pipe.ZRem(ctx, s.runsKey(), run.ID.String())
			pipe.ZRem(ctx, s.runStatusKey(run.Status), run.ID.String())
			pipe.HIncrBy(ctx, s.pipelineCountsKey(), countField(run.PipelineName, string(run.Status)), -1)
			if run.IdempotencyKey != "" {
				pipe.Del(ctx, s.pipelineIdempotencyKey(run.PipelineName, run.IdempotencyKey))
			}
			result.PipelineRuns++
		}
		return nil
	})
	return result, err
}

func (s *Store) purgeExecution(ctx context.Context, pipe redislib.Pipeliner, item execution.Execution) {
	pipe.Del(ctx, s.executionKey(item.ID), s.eventsKey(item.ID))
	pipe.ZRem(ctx, s.executionsKey(), item.ID.String())
	pipe.ZRem(ctx, s.taskIndexKey(item.TaskName), item.ID.String())
	pipe.ZRem(ctx, s.statusKey(item.Status), item.ID.String())
	pipe.ZRem(ctx, s.queueKey(item.TaskName), item.ID.String())
	pipe.ZRem(ctx, s.leasesKey(), item.ID.String())
	pipe.HIncrBy(ctx, s.executionCountsKey(), countField(item.TaskName, string(item.Status)), -1)
	if item.IdempotencyKey != "" {
		pipe.Del(ctx, s.idempotencyKey(item.TaskName, item.IdempotencyKey))
	}
	if item.PipelineRunID != nil {
		pipe.Del(ctx, s.nodeKey(*item.PipelineRunID, item.NodeKey))
		pipe.ZRem(ctx, s.runExecutionsKey(*item.PipelineRunID), item.ID.String())
	}
}

func countField(name, status string) string { return name + "\x00" + status }

func parseCount(field, rawCount string) (string, string, int64, error) {
	parts := strings.SplitN(field, "\x00", 2)
	if len(parts) != 2 {
		return "", "", 0, fmt.Errorf("invalid redis count field %q", field)
	}
	count, err := strconv.ParseInt(rawCount, 10, 64)
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid redis count %q: %w", rawCount, err)
	}
	return parts[0], parts[1], count, nil
}

func encodeEvent(item execution.Execution, eventType execution.EventType, errorText string, now time.Time) ([]byte, error) {
	event := execution.Event{
		ID: uuid.New(), ExecutionID: item.ID, Type: eventType, Attempt: item.Attempt,
		Error: errorText, CreatedAt: now.UTC(),
	}
	return json.Marshal(event)
}

func (s *Store) getExecutionTx(ctx context.Context, tx *redislib.Tx, id uuid.UUID) (execution.Execution, error) {
	value, err := tx.Get(ctx, s.executionKey(id)).Bytes()
	if errors.Is(err, redislib.Nil) {
		return execution.Execution{}, execution.ErrNotFound
	}
	if err != nil {
		return execution.Execution{}, err
	}
	var item execution.Execution
	if err = json.Unmarshal(value, &item); err != nil {
		return execution.Execution{}, err
	}
	return item, nil
}

// watch retries optimistic transactions when another process changes a watched source record.
// The bound avoids an unbounded busy loop under sustained contention.
func (s *Store) watch(ctx context.Context, keys []string, operation func(*redislib.Tx) error) error {
	for range 16 {
		err := s.client.Watch(ctx, operation, keys...)
		if !errors.Is(err, redislib.TxFailedErr) {
			return err
		}
	}
	return errors.New("redis transaction contention limit exceeded")
}

// now returns server time, which is the shared clock for all distributed workers.
func (s *Store) now(ctx context.Context) (time.Time, error) {
	value, err := s.client.Time(ctx).Result()
	return value.UTC(), err
}
