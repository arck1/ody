package redis

import (
	"context"
	"sort"

	"github.com/google/uuid"
	redislib "github.com/redis/go-redis/v9"

	"schedulor/execution"
)

// CreatePipelineRun persists a new pending run and its list indexes.
func (s *Store) CreatePipelineRun(ctx context.Context, request execution.CreatePipelineRun) (execution.PipelineRun, error) {
	now, err := s.now(ctx)
	if err != nil {
		return execution.PipelineRun{}, err
	}
	run := execution.PipelineRun{
		ID: uuid.New(), PipelineName: request.PipelineName, PipelineVersion: request.PipelineVersion,
		Input: cloneJSON(request.Input), Status: execution.RunPending, CreatedAt: now, UpdatedAt: now,
	}
	_, err = s.client.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
		pipe.HSet(ctx, s.pipelineKey(run.ID), encodeRun(run))
		pipe.ZAdd(ctx, s.runsKey(), redislib.Z{Score: score(now), Member: run.ID.String()})
		pipe.ZAdd(ctx, s.runStatusKey(run.Status), redislib.Z{Score: score(now), Member: run.ID.String()})
		return nil
	})
	return run, err
}

// GetPipelineRun reads one pipeline source-of-truth hash.
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

// ListPipelineRuns returns newest runs first and optionally unions status indexes.
func (s *Store) ListPipelineRuns(ctx context.Context, statuses []execution.RunStatus) ([]execution.PipelineRun, error) {
	ids, err := s.pipelineIDs(ctx, statuses)
	if err != nil {
		return nil, err
	}
	items := make([]execution.PipelineRun, 0, len(ids))
	for value := range ids {
		id, parseErr := uuid.Parse(value)
		if parseErr != nil {
			return nil, parseErr
		}
		run, getErr := s.GetPipelineRun(ctx, id)
		if getErr != nil {
			return nil, getErr
		}
		items = append(items, run)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	return items, nil
}

func (s *Store) pipelineIDs(ctx context.Context, statuses []execution.RunStatus) (map[string]struct{}, error) {
	ids := make(map[string]struct{})
	keys := []string{s.runsKey()}
	if len(statuses) > 0 {
		keys = make([]string, len(statuses))
		for index, status := range statuses {
			keys[index] = s.runStatusKey(status)
		}
	}
	for _, key := range keys {
		values, err := s.client.ZRevRange(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			ids[value] = struct{}{}
		}
	}
	return ids, nil
}

// SetPipelineRunStatus updates the run hash and moves it between status indexes atomically.
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
