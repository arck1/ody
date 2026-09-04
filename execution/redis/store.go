// Package redis implements execution.Store with Redis.
package redis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	redislib "github.com/redis/go-redis/v9"

	"schedulor/execution"
)

const defaultPrefix = "schedulor:{execution}:"

// Options configures Redis key names. Prefix should contain a common Redis Cluster hash tag.
type Options struct {
	Prefix string
}

// Store persists the complete execution model and uses sorted sets as the delivery queue.
// The caller owns and closes the Redis client.
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

func (s *Store) ListExecutions(ctx context.Context, filter execution.ListFilter) ([]execution.Execution, error) {
	key := s.executionsKey()
	if filter.PipelineRunID != nil {
		key = s.runExecutionsKey(*filter.PipelineRunID)
	} else if filter.TaskName != "" {
		key = s.taskIndexKey(filter.TaskName)
	} else if filter.Status != "" {
		key = s.statusKey(filter.Status)
	}
	ids, err := s.client.ZRevRange(ctx, key, 0, -1).Result()
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
		if filter.TaskName != "" && item.TaskName != filter.TaskName || filter.Status != "" && item.Status != filter.Status ||
			filter.PipelineRunID != nil && (item.PipelineRunID == nil || *item.PipelineRunID != *filter.PipelineRunID) {
			continue
		}
		items = append(items, item)
		if filter.Limit > 0 && len(items) == filter.Limit {
			break
		}
	}
	return items, nil
}

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

func (s *Store) Claim(ctx context.Context, owner string, names []string, limit int, lease time.Duration) ([]execution.Execution, error) {
	if limit <= 0 {
		limit = 1
	}
	now, err := s.now(ctx)
	if err != nil {
		return nil, err
	}
	accepted := make(map[string]struct{}, len(names))
	candidates := make(map[uuid.UUID]execution.Execution)
	for _, name := range names {
		accepted[name] = struct{}{}
		values, rangeErr := s.client.ZRangeByScore(ctx, s.queueKey(name), &redislib.ZRangeBy{
			Min: "-inf", Max: fmt.Sprint(score(now)), Count: int64(max(limit*8, 128)),
		}).Result()
		if rangeErr != nil {
			return nil, rangeErr
		}
		for _, value := range values {
			id, parseErr := uuid.Parse(value)
			if parseErr != nil {
				return nil, parseErr
			}
			item, getErr := s.GetExecution(ctx, id)
			if errors.Is(getErr, execution.ErrNotFound) {
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
	claimed := make([]execution.Execution, 0, limit)
	for _, candidate := range ordered {
		if len(claimed) == limit {
			break
		}
		var result execution.Execution
		claimErr := s.watch(ctx, []string{s.executionKey(candidate.ID)}, func(tx *redislib.Tx) error {
			item, getErr := s.getExecutionTx(ctx, tx, candidate.ID)
			if getErr != nil {
				return getErr
			}
			_, nameAccepted := accepted[item.TaskName]
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
			if txErr := s.saveTransition(ctx, tx, item, previous, execution.EventStarted, "", now, true); txErr != nil {
				return txErr
			}
			result = item
			return nil
		})
		if errors.Is(claimErr, errUnavailable) || errors.Is(claimErr, execution.ErrNotFound) {
			continue
		}
		if claimErr != nil {
			return nil, claimErr
		}
		claimed = append(claimed, result)
	}
	return claimed, nil
}

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
		var runID *uuid.UUID
		reapErr := s.watch(ctx, []string{s.executionKey(id)}, func(tx *redislib.Tx) error {
			item, getErr := s.getExecutionTx(ctx, tx, id)
			if errors.Is(getErr, execution.ErrNotFound) {
				_, getErr = tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
					pipe.ZRem(ctx, s.leasesKey(), id.String())
					return nil
				})
				return getErr
			}
			if getErr != nil {
				return getErr
			}
			if item.Status != execution.StatusRunning || item.LeaseUntil.After(now) {
				return errUnavailable
			}
			if item.Attempt < item.MaxAttempts {
				_, getErr = tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
					pipe.ZRem(ctx, s.leasesKey(), id.String())
					return nil
				})
				return getErr
			}
			previous := item.Status
			item.Status, item.LastError, item.FinishedAt = execution.StatusFailed, "lease expired after maximum attempts", new(now)
			clearLease(&item)
			runID = cloneUUID(item.PipelineRunID)
			return s.saveTransition(ctx, tx, item, previous, execution.EventFailed, item.LastError, now, false)
		})
		if reapErr != nil && !errors.Is(reapErr, errUnavailable) && !errors.Is(reapErr, execution.ErrNotFound) {
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

func (s *Store) Succeed(ctx context.Context, id, token uuid.UUID, output json.RawMessage) error {
	return s.transition(ctx, id, token, func(item *execution.Execution, now time.Time) (execution.EventType, string, bool) {
		item.Status, item.Output, item.FinishedAt = execution.StatusSucceeded, cloneJSON(output), new(now)
		clearLease(item)
		return execution.EventSucceeded, "", false
	})
}

func (s *Store) Retry(ctx context.Context, id, token uuid.UUID, errorText string, availableAt time.Time) error {
	return s.transition(ctx, id, token, func(item *execution.Execution, now time.Time) (execution.EventType, string, bool) {
		item.LastError = errorText
		clearLease(item)
		if item.Attempt >= item.MaxAttempts {
			item.Status, item.FinishedAt = execution.StatusFailed, new(now)
			return execution.EventFailed, errorText, false
		}
		item.Status, item.AvailableAt = execution.StatusRetry, availableAt.UTC()
		return execution.EventRetried, errorText, true
	})
}

func (s *Store) Fail(ctx context.Context, id, token uuid.UUID, errorText string) error {
	return s.transition(ctx, id, token, func(item *execution.Execution, now time.Time) (execution.EventType, string, bool) {
		item.Status, item.LastError, item.FinishedAt = execution.StatusFailed, errorText, new(now)
		clearLease(item)
		return execution.EventFailed, errorText, false
	})
}

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
			pipe.ZRem(ctx, s.statusKey(previous), id.String())
			pipe.ZAdd(ctx, s.statusKey(item.Status), redislib.Z{Score: score(item.CreatedAt), Member: id.String()})
			pipe.ZRem(ctx, s.leasesKey(), id.String())
			pipe.ZAdd(ctx, s.queueKey(item.TaskName), redislib.Z{Score: score(item.AvailableAt), Member: id.String()})
			pipe.RPush(ctx, s.eventsKey(id), event)
			if item.PipelineRunID != nil {
				pipe.HSet(ctx, s.pipelineKey(*item.PipelineRunID), "status", string(execution.RunRunning), "error", "", "updated_at", now.Format(time.RFC3339Nano), "finished_at", "")
				pipe.ZRem(ctx, s.runStatusKey(execution.RunSucceeded), item.PipelineRunID.String())
				pipe.ZRem(ctx, s.runStatusKey(execution.RunFailed), item.PipelineRunID.String())
				pipe.ZRem(ctx, s.runStatusKey(execution.RunCancelled), item.PipelineRunID.String())
				pipe.ZAdd(ctx, s.runStatusKey(execution.RunRunning), redislib.Z{Score: score(now), Member: item.PipelineRunID.String()})
			}
			return nil
		})
		result = item
		return getErr
	})
	return result, err
}

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

func (s *Store) CreatePipelineRun(ctx context.Context, request execution.CreatePipelineRun) (execution.PipelineRun, error) {
	now, err := s.now(ctx)
	if err != nil {
		return execution.PipelineRun{}, err
	}
	run := execution.PipelineRun{ID: uuid.New(), PipelineName: request.PipelineName, PipelineVersion: request.PipelineVersion, Input: cloneJSON(request.Input), Status: execution.RunPending, CreatedAt: now, UpdatedAt: now}
	_, err = s.client.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
		pipe.HSet(ctx, s.pipelineKey(run.ID), encodeRun(run))
		pipe.ZAdd(ctx, s.runsKey(), redislib.Z{Score: score(now), Member: run.ID.String()})
		pipe.ZAdd(ctx, s.runStatusKey(run.Status), redislib.Z{Score: score(now), Member: run.ID.String()})
		return nil
	})
	return run, err
}

func (s *Store) GetPipelineRun(ctx context.Context, id uuid.UUID) (execution.PipelineRun, error) {
	values, err := s.client.HGetAll(ctx, s.pipelineKey(id)).Result()
	if err != nil {
		return execution.PipelineRun{}, err
	}
	if len(values) == 0 {
		return execution.PipelineRun{}, execution.ErrNotFound
	}
	return decodeRun(values)
}

func (s *Store) ListPipelineRuns(ctx context.Context, statuses []execution.RunStatus) ([]execution.PipelineRun, error) {
	ids := make(map[string]struct{})
	if len(statuses) == 0 {
		values, err := s.client.ZRevRange(ctx, s.runsKey(), 0, -1).Result()
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			ids[value] = struct{}{}
		}
	} else {
		for _, status := range statuses {
			values, err := s.client.ZRevRange(ctx, s.runStatusKey(status), 0, -1).Result()
			if err != nil {
				return nil, err
			}
			for _, value := range values {
				ids[value] = struct{}{}
			}
		}
	}
	items := make([]execution.PipelineRun, 0, len(ids))
	for value := range ids {
		id, err := uuid.Parse(value)
		if err != nil {
			return nil, err
		}
		run, err := s.GetPipelineRun(ctx, id)
		if err != nil {
			return nil, err
		}
		items = append(items, run)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	return items, nil
}

func (s *Store) SetPipelineRunStatus(ctx context.Context, id uuid.UUID, status execution.RunStatus, errorText string) error {
	now, err := s.now(ctx)
	if err != nil {
		return err
	}
	return s.watch(ctx, []string{s.pipelineKey(id)}, func(tx *redislib.Tx) error {
		values, getErr := tx.HGetAll(ctx, s.pipelineKey(id)).Result()
		if getErr != nil {
			return getErr
		}
		if len(values) == 0 {
			return execution.ErrNotFound
		}
		run, getErr := decodeRun(values)
		if getErr != nil {
			return getErr
		}
		previous := run.Status
		run.Status, run.Error, run.UpdatedAt = status, errorText, now
		if status == execution.RunSucceeded || status == execution.RunFailed || status == execution.RunCancelled {
			run.FinishedAt = new(now)
		} else {
			run.FinishedAt = nil
		}
		_, getErr = tx.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
			pipe.HSet(ctx, s.pipelineKey(id), encodeRun(run))
			pipe.ZRem(ctx, s.runStatusKey(previous), id.String())
			pipe.ZAdd(ctx, s.runStatusKey(status), redislib.Z{Score: score(run.CreatedAt), Member: id.String()})
			return nil
		})
		return getErr
	})
}

var (
	errExisting    = errors.New("redis execution reference already exists")
	errUnavailable = errors.New("redis execution is not claimable")
)

func (s *Store) saveTransition(ctx context.Context, tx *redislib.Tx, item execution.Execution, previous execution.Status, eventType execution.EventType, errorText string, now time.Time, queued bool) error {
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
		pipe.ZRem(ctx, s.statusKey(previous), item.ID.String())
		pipe.ZAdd(ctx, s.statusKey(item.Status), redislib.Z{Score: score(item.CreatedAt), Member: item.ID.String()})
		pipe.ZRem(ctx, s.leasesKey(), item.ID.String())
		if queued {
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

func (s *Store) watch(ctx context.Context, keys []string, operation func(*redislib.Tx) error) error {
	for range 16 {
		err := s.client.Watch(ctx, operation, keys...)
		if !errors.Is(err, redislib.TxFailedErr) {
			return err
		}
	}
	return errors.New("redis transaction contention limit exceeded")
}

func (s *Store) now(ctx context.Context) (time.Time, error) {
	value, err := s.client.Time(ctx).Result()
	return value.UTC(), err
}

func encodeRun(run execution.PipelineRun) map[string]any {
	finished := ""
	if run.FinishedAt != nil {
		finished = run.FinishedAt.UTC().Format(time.RFC3339Nano)
	}
	return map[string]any{
		"id": run.ID.String(), "pipeline_name": run.PipelineName, "pipeline_version": run.PipelineVersion,
		"input": string(run.Input), "status": string(run.Status), "error": run.Error,
		"created_at": run.CreatedAt.UTC().Format(time.RFC3339Nano), "updated_at": run.UpdatedAt.UTC().Format(time.RFC3339Nano),
		"finished_at": finished,
	}
}

func decodeRun(values map[string]string) (execution.PipelineRun, error) {
	id, err := uuid.Parse(values["id"])
	if err != nil {
		return execution.PipelineRun{}, err
	}
	version := 0
	if _, err = fmt.Sscan(values["pipeline_version"], &version); err != nil {
		return execution.PipelineRun{}, err
	}
	created, err := time.Parse(time.RFC3339Nano, values["created_at"])
	if err != nil {
		return execution.PipelineRun{}, err
	}
	updated, err := time.Parse(time.RFC3339Nano, values["updated_at"])
	if err != nil {
		return execution.PipelineRun{}, err
	}
	run := execution.PipelineRun{
		ID: id, PipelineName: values["pipeline_name"], PipelineVersion: version,
		Input: json.RawMessage(values["input"]), Status: execution.RunStatus(values["status"]), Error: values["error"],
		CreatedAt: created, UpdatedAt: updated,
	}
	if values["finished_at"] != "" {
		finished, parseErr := time.Parse(time.RFC3339Nano, values["finished_at"])
		if parseErr != nil {
			return execution.PipelineRun{}, parseErr
		}
		run.FinishedAt = &finished
	}
	return run, nil
}

func clearLease(item *execution.Execution) {
	item.LeaseOwner, item.LeaseToken, item.LeaseUntil = "", uuid.Nil, time.Time{}
}

func score(value time.Time) float64 { return float64(value.UnixMilli()) }
func cloneJSON(value json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), value...)
}
func cloneUUID(value *uuid.UUID) *uuid.UUID {
	if value == nil {
		return nil
	}
	return new(*value)
}

func encodeKey(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func (s *Store) executionKey(id uuid.UUID) string { return s.prefix + "execution:" + id.String() }
func (s *Store) eventsKey(id uuid.UUID) string    { return s.prefix + "events:" + id.String() }
func (s *Store) executionsKey() string            { return s.prefix + "executions" }
func (s *Store) leasesKey() string                { return s.prefix + "leases" }
func (s *Store) queueKey(name string) string      { return s.prefix + "queue:" + encodeKey(name) }
func (s *Store) taskIndexKey(name string) string  { return s.prefix + "task:" + encodeKey(name) }
func (s *Store) statusKey(status execution.Status) string {
	return s.prefix + "status:" + string(status)
}
func (s *Store) idempotencyKey(taskName, key string) string {
	return s.prefix + "idempotency:" + encodeKey(taskName+"\x00"+key)
}
func (s *Store) nodeKey(runID uuid.UUID, node string) string {
	return s.prefix + "node:" + runID.String() + ":" + encodeKey(node)
}
func (s *Store) runExecutionsKey(id uuid.UUID) string {
	return s.prefix + "run-executions:" + id.String()
}
func (s *Store) pipelineKey(id uuid.UUID) string { return s.prefix + "pipeline:" + id.String() }
func (s *Store) runsKey() string                 { return s.prefix + "pipelines" }
func (s *Store) runStatusKey(status execution.RunStatus) string {
	return s.prefix + "pipeline-status:" + string(status)
}
