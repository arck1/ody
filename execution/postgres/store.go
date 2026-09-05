package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"schedulor/execution"
)

//go:embed schema.sql
var schema string

// Store implements execution.Store using one application-owned SQL connection pool.
type Store struct{ db *sql.DB }

func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("postgres execution store db is nil")
	}
	return &Store{db: db}, nil
}

// Migrate applies the embedded schema. Every statement is safe to execute repeatedly.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

const executionColumns = `id, task_name, task_version, input, output, status, attempt, max_attempts,
available_at, lease_owner, lease_token, lease_until, last_error, idempotency_key,
pipeline_run_id, node_key, created_at, started_at, finished_at`

func (s *Store) CreateExecution(ctx context.Context, request execution.CreateExecution) (execution.Execution, error) {
	now := time.Now().UTC()
	if request.AvailableAt.IsZero() {
		request.AvailableAt = now
	}
	if request.MaxAttempts <= 0 {
		request.MaxAttempts = 1
	}
	id := uuid.New()
	var idem any
	if request.IdempotencyKey != "" {
		idem = request.IdempotencyKey
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return execution.Execution{}, err
	}
	defer func() { _ = tx.Rollback() }()
	row := tx.QueryRowContext(ctx, `INSERT INTO task_executions
        (id, task_name, task_version, input, status, max_attempts, available_at, idempotency_key, pipeline_run_id, node_key, created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING RETURNING `+executionColumns,
		id, request.TaskName, request.TaskVersion, []byte(request.Input), execution.StatusPending, request.MaxAttempts, request.AvailableAt, idem, request.PipelineRunID, request.NodeKey, now)
	item, err := scanExecution(row)
	if err == nil {
		if eventErr := s.insertEvent(ctx, tx, item, execution.EventCreated, ""); eventErr != nil {
			return execution.Execution{}, eventErr
		}
		if err = tx.Commit(); err != nil {
			return execution.Execution{}, err
		}
		return item, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return execution.Execution{}, err
	}
	if err = tx.Rollback(); err != nil {
		return execution.Execution{}, err
	}
	conditions := make([]string, 0, 2)
	args := []any{}
	if request.IdempotencyKey != "" {
		args = append(args, request.TaskName, request.IdempotencyKey)
		conditions = append(conditions, fmt.Sprintf("(task_name=$%d AND idempotency_key=$%d)", len(args)-1, len(args)))
	}
	if request.PipelineRunID != nil && request.NodeKey != "" {
		args = append(args, request.PipelineRunID, request.NodeKey)
		conditions = append(conditions, fmt.Sprintf("(pipeline_run_id=$%d AND node_key=$%d)", len(args)-1, len(args)))
	}
	if len(conditions) == 0 {
		return execution.Execution{}, errors.New("execution insert conflicted without an idempotency key")
	}
	return scanExecution(s.db.QueryRowContext(ctx, `SELECT `+executionColumns+` FROM task_executions WHERE `+strings.Join(conditions, " OR ")+` LIMIT 1`, args...))
}

func (s *Store) GetExecution(ctx context.Context, id uuid.UUID) (execution.Execution, error) {
	item, err := scanExecution(s.db.QueryRowContext(ctx, `SELECT `+executionColumns+` FROM task_executions WHERE id=$1`, id))
	return item, translateNotFound(err)
}

func (s *Store) ListExecutions(ctx context.Context, filter execution.ListFilter) ([]execution.Execution, error) {
	conditions := []string{"TRUE"}
	args := []any{}
	if filter.TaskName != "" {
		args = append(args, filter.TaskName)
		conditions = append(conditions, fmt.Sprintf("task_name=$%d", len(args)))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		conditions = append(conditions, fmt.Sprintf("status=$%d", len(args)))
	}
	if filter.PipelineRunID != nil {
		args = append(args, filter.PipelineRunID)
		conditions = append(conditions, fmt.Sprintf("pipeline_run_id=$%d", len(args)))
	}
	if filter.Before != nil {
		args = append(args, filter.Before.CreatedAt, filter.Before.ID)
		conditions = append(conditions, fmt.Sprintf("(created_at,id)<($%d,$%d)", len(args)-1, len(args)))
	}
	query := `SELECT ` + executionColumns + ` FROM task_executions WHERE ` + strings.Join(conditions, " AND ") + ` ORDER BY created_at DESC,id DESC`
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []execution.Execution{}
	for rows.Next() {
		item, scanErr := scanExecution(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListRunExecutions(ctx context.Context, runID uuid.UUID) ([]execution.Execution, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+executionColumns+` FROM task_executions WHERE pipeline_run_id=$1 ORDER BY created_at,id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []execution.Execution{}
	for rows.Next() {
		item, scanErr := scanExecution(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) Claim(ctx context.Context, owner string, keys []execution.TaskKey, limit int, lease time.Duration) ([]execution.Execution, error) {
	if limit <= 0 {
		limit = 1
	}
	if len(keys) == 0 {
		return nil, nil
	}
	routing := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys)*2+3)
	for _, key := range keys {
		routing = append(routing, fmt.Sprintf("(task_name=$%d AND task_version=$%d)", len(args)+1, len(args)+2))
		args = append(args, key.Name, key.Version)
	}
	limitParam, ownerParam, leaseParam := len(args)+1, len(args)+2, len(args)+3
	args = append(args, limit, owner, lease.Milliseconds())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	query := fmt.Sprintf(`WITH candidates AS (
        SELECT id FROM task_executions
        WHERE (%s) AND available_at <= now()
          AND ((status IN ('pending','retry')) OR (status='running' AND lease_until <= now()))
          AND attempt < max_attempts
		ORDER BY available_at,created_at FOR UPDATE SKIP LOCKED LIMIT $%d)
	  UPDATE task_executions e SET status='running', attempt=e.attempt+1, lease_owner=$%d,
		lease_token=gen_random_uuid(), lease_until=now()+($%d*interval '1 millisecond'), started_at=COALESCE(started_at,now())
	  FROM candidates c WHERE e.id=c.id RETURNING %s`, strings.Join(routing, " OR "), limitParam, ownerParam, leaseParam, prefixedColumns("e"))
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	items := []execution.Execution{}
	for rows.Next() {
		item, scanErr := scanExecution(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	for _, item := range items {
		if err = s.insertEvent(ctx, tx, item, execution.EventStarted, ""); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) ReapExpired(ctx context.Context) ([]uuid.UUID, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT `+executionColumns+` FROM task_executions WHERE status='running' AND lease_until<=now() AND attempt>=max_attempts FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	items := []execution.Execution{}
	for rows.Next() {
		item, scanErr := scanExecution(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	runs := map[uuid.UUID]struct{}{}
	for _, item := range items {
		item.LastError = "lease expired after maximum attempts"
		if _, err = tx.ExecContext(ctx, `UPDATE task_executions SET status='failed',last_error=$2,lease_owner='',lease_token=NULL,lease_until=NULL,finished_at=now() WHERE id=$1`, item.ID, item.LastError); err != nil {
			return nil, err
		}
		if err = s.insertEvent(ctx, tx, item, execution.EventFailed, item.LastError); err != nil {
			return nil, err
		}
		if item.PipelineRunID != nil {
			runs[*item.PipelineRunID] = struct{}{}
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(runs))
	for id := range runs {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *Store) Heartbeat(ctx context.Context, id, token uuid.UUID, lease time.Duration) error {
	result, err := s.db.ExecContext(ctx, `UPDATE task_executions SET lease_until=now()+($3*interval '1 millisecond') WHERE id=$1 AND lease_token=$2 AND status='running'`, id, token, lease.Milliseconds())
	return ownedResult(result, err)
}

func (s *Store) Succeed(ctx context.Context, id, token uuid.UUID, output json.RawMessage) error {
	return s.transition(ctx, id, token, execution.StatusSucceeded, execution.EventSucceeded, "", output, time.Time{})
}
func (s *Store) Retry(ctx context.Context, id, token uuid.UUID, text string, delay time.Duration) error {
	var databaseNow time.Time
	if err := s.db.QueryRowContext(ctx, `SELECT now()`).Scan(&databaseNow); err != nil {
		return err
	}
	return s.transition(ctx, id, token, execution.StatusRetry, execution.EventRetried, text, nil, databaseNow.Add(delay))
}
func (s *Store) Fail(ctx context.Context, id, token uuid.UUID, text string) error {
	return s.transition(ctx, id, token, execution.StatusFailed, execution.EventFailed, text, nil, time.Time{})
}

func (s *Store) transition(ctx context.Context, id, token uuid.UUID, status execution.Status, event execution.EventType, text string, output json.RawMessage, available time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	query := `UPDATE task_executions SET status=$3,last_error=$4,output=COALESCE($5,output),lease_owner='',lease_token=NULL,lease_until=NULL,finished_at=CASE WHEN $3 IN ('succeeded','failed','cancelled') THEN now() ELSE NULL END,available_at=CASE WHEN $3='retry' THEN $6 ELSE available_at END WHERE id=$1 AND lease_token=$2 AND status='running' RETURNING ` + executionColumns
	var outputArg any
	if output != nil {
		outputArg = []byte(output)
	}
	var availableArg any
	if !available.IsZero() {
		availableArg = available
	}
	item, err := scanExecution(tx.QueryRowContext(ctx, query, id, token, status, text, outputArg, availableArg))
	if errors.Is(err, sql.ErrNoRows) {
		return execution.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if item.Attempt >= item.MaxAttempts && status == execution.StatusRetry {
		item, err = scanExecution(tx.QueryRowContext(ctx, `UPDATE task_executions SET status='failed',finished_at=now() WHERE id=$1 RETURNING `+executionColumns, id))
		event = execution.EventFailed
		if err != nil {
			return err
		}
	}
	if err = s.insertEvent(ctx, tx, item, event, text); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CancelExecution(ctx context.Context, id uuid.UUID, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	item, err := scanExecution(tx.QueryRowContext(ctx, `UPDATE task_executions SET status='cancelled',last_error=$2,lease_owner='',lease_token=NULL,lease_until=NULL,finished_at=now() WHERE id=$1 AND status NOT IN ('succeeded','failed','cancelled') RETURNING `+executionColumns, id, reason))
	if errors.Is(err, sql.ErrNoRows) {
		return execution.ErrNotFound
	}
	if err != nil {
		return err
	}
	if err = s.insertEvent(ctx, tx, item, execution.EventCancelled, reason); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RestartExecution(ctx context.Context, id uuid.UUID, availableAt time.Time) (execution.Execution, error) {
	if availableAt.IsZero() {
		availableAt = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return execution.Execution{}, err
	}
	defer func() { _ = tx.Rollback() }()
	item, err := scanExecution(tx.QueryRowContext(ctx, `UPDATE task_executions SET
        status='pending',attempt=0,available_at=$2,output=NULL,last_error='',started_at=NULL,finished_at=NULL,
        lease_owner='',lease_token=NULL,lease_until=NULL
        WHERE id=$1 AND pipeline_run_id IS NULL AND status IN ('succeeded','failed','cancelled') RETURNING `+executionColumns, id, availableAt))
	if errors.Is(err, sql.ErrNoRows) {
		var status execution.Status
		var pipelineRunID *uuid.UUID
		lookupErr := tx.QueryRowContext(ctx, `SELECT status,pipeline_run_id FROM task_executions WHERE id=$1`, id).Scan(&status, &pipelineRunID)
		if errors.Is(lookupErr, sql.ErrNoRows) {
			return execution.Execution{}, execution.ErrNotFound
		}
		if lookupErr != nil {
			return execution.Execution{}, lookupErr
		}
		if pipelineRunID != nil {
			return execution.Execution{}, execution.ErrPipelineExecution
		}
		return execution.Execution{}, execution.ErrActive
	}
	if err != nil {
		return execution.Execution{}, err
	}
	if err = s.insertEvent(ctx, tx, item, execution.EventRestarted, ""); err != nil {
		return execution.Execution{}, err
	}
	return item, tx.Commit()
}

func (s *Store) RestartPipelineSubgraph(ctx context.Context, request execution.RestartSubgraph) (execution.Execution, error) {
	if request.AvailableAt.IsZero() {
		request.AvailableAt = time.Now().UTC()
	}
	keys := append([]string{request.RootNodeKey}, request.DescendantKeys...)
	placeholders := make([]string, len(keys))
	args := make([]any, 0, len(keys)+2)
	args = append(args, request.RunID)
	for index, key := range keys {
		args = append(args, key)
		placeholders[index] = fmt.Sprintf("$%d", len(args))
	}
	availableParam := len(args) + 1
	args = append(args, request.AvailableAt)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return execution.Execution{}, err
	}
	defer func() { _ = tx.Rollback() }()
	query := fmt.Sprintf(`SELECT %s FROM task_executions WHERE pipeline_run_id=$1 AND node_key IN (%s) FOR UPDATE`, executionColumns, strings.Join(placeholders, ","))
	rows, err := tx.QueryContext(ctx, query, args[:len(args)-1]...)
	if err != nil {
		return execution.Execution{}, err
	}
	items := make([]execution.Execution, 0, len(keys))
	for rows.Next() {
		item, scanErr := scanExecution(rows)
		if scanErr != nil {
			_ = rows.Close()
			return execution.Execution{}, scanErr
		}
		if item.Status == execution.StatusPending || item.Status == execution.StatusRetry || item.Status == execution.StatusRunning {
			_ = rows.Close()
			return execution.Execution{}, execution.ErrActive
		}
		items = append(items, item)
	}
	if err = rows.Close(); err != nil {
		return execution.Execution{}, err
	}
	var root execution.Execution
	foundRoot := false
	for _, item := range items {
		if item.NodeKey == request.RootNodeKey {
			root, foundRoot = item, true
		}
	}
	if !foundRoot {
		return execution.Execution{}, execution.ErrNotFound
	}
	update := fmt.Sprintf(`UPDATE task_executions SET
		status=CASE WHEN node_key=$2 THEN 'pending' ELSE 'blocked' END,
		attempt=0,available_at=$%d,output=NULL,last_error='',started_at=NULL,finished_at=NULL,
		lease_owner='',lease_token=NULL,lease_until=NULL
		WHERE pipeline_run_id=$1 AND node_key IN (%s) RETURNING %s`, availableParam, strings.Join(placeholders, ","), executionColumns)
	updatedRows, err := tx.QueryContext(ctx, update, args...)
	if err != nil {
		return execution.Execution{}, err
	}
	updated := make([]execution.Execution, 0, len(items))
	for updatedRows.Next() {
		item, scanErr := scanExecution(updatedRows)
		if scanErr != nil {
			_ = updatedRows.Close()
			return execution.Execution{}, scanErr
		}
		updated = append(updated, item)
		if item.NodeKey == request.RootNodeKey {
			root = item
		}
	}
	if err = updatedRows.Close(); err != nil {
		return execution.Execution{}, err
	}
	for _, item := range updated {
		if err = s.insertEvent(ctx, tx, item, execution.EventRestarted, ""); err != nil {
			return execution.Execution{}, err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE pipeline_runs SET status='running',error='',updated_at=now(),finished_at=NULL WHERE id=$1`, request.RunID)
	if err != nil {
		return execution.Execution{}, err
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected == 0 {
		if affectedErr != nil {
			return execution.Execution{}, affectedErr
		}
		return execution.Execution{}, execution.ErrNotFound
	}
	return root, tx.Commit()
}

func (s *Store) ReleaseExecution(ctx context.Context, id uuid.UUID, input json.RawMessage, availableAt time.Time) error {
	if availableAt.IsZero() {
		availableAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE task_executions SET status='pending',input=$2,available_at=$3 WHERE id=$1 AND status='blocked'`, id, []byte(input), availableAt)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return execution.ErrActive
	}
	return nil
}

func (s *Store) Events(ctx context.Context, id uuid.UUID) ([]execution.Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,execution_id,event_type,attempt,error,created_at FROM execution_events WHERE execution_id=$1 ORDER BY created_at,id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []execution.Event{}
	for rows.Next() {
		var item execution.Event
		if err = rows.Scan(&item.ID, &item.ExecutionID, &item.Type, &item.Attempt, &item.Error, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ExecutionCounts(ctx context.Context) ([]execution.ExecutionCount, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT task_name,status,count(*) FROM task_executions GROUP BY task_name,status ORDER BY task_name,status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []execution.ExecutionCount{}
	for rows.Next() {
		var item execution.ExecutionCount
		if err = rows.Scan(&item.TaskName, &item.Status, &item.Count); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) PipelineCounts(ctx context.Context) ([]execution.PipelineCount, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT pipeline_name,status,count(*) FROM pipeline_runs GROUP BY pipeline_name,status ORDER BY pipeline_name,status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []execution.PipelineCount{}
	for rows.Next() {
		var item execution.PipelineCount
		if err = rows.Scan(&item.PipelineName, &item.Status, &item.Count); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) Purge(ctx context.Context, before time.Time, limit int) (execution.PurgeResult, error) {
	if limit <= 0 {
		limit = 1000
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return execution.PurgeResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	runRows, err := tx.QueryContext(ctx, `SELECT id FROM pipeline_runs
		WHERE finished_at IS NOT NULL AND finished_at<$1 ORDER BY finished_at,id LIMIT $2 FOR UPDATE SKIP LOCKED`, before, limit)
	if err != nil {
		return execution.PurgeResult{}, err
	}
	runIDs := []uuid.UUID{}
	for runRows.Next() {
		var id uuid.UUID
		if err = runRows.Scan(&id); err != nil {
			_ = runRows.Close()
			return execution.PurgeResult{}, err
		}
		runIDs = append(runIDs, id)
	}
	if err = runRows.Close(); err != nil {
		return execution.PurgeResult{}, err
	}
	result := execution.PurgeResult{PipelineRuns: int64(len(runIDs))}
	if len(runIDs) > 0 {
		deleted, deleteErr := tx.ExecContext(ctx, `DELETE FROM task_executions WHERE pipeline_run_id=ANY($1)`, runIDs)
		if deleteErr != nil {
			return execution.PurgeResult{}, deleteErr
		}
		result.Executions, err = deleted.RowsAffected()
		if err != nil {
			return execution.PurgeResult{}, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM pipeline_runs WHERE id=ANY($1)`, runIDs); err != nil {
			return execution.PurgeResult{}, err
		}
	}
	remaining := limit - len(runIDs)
	if remaining > 0 {
		deleted, deleteErr := tx.ExecContext(ctx, `DELETE FROM task_executions WHERE id IN (
			SELECT id FROM task_executions WHERE pipeline_run_id IS NULL AND finished_at IS NOT NULL
			AND finished_at<$1 ORDER BY finished_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)`, before, remaining)
		if deleteErr != nil {
			return execution.PurgeResult{}, deleteErr
		}
		count, countErr := deleted.RowsAffected()
		if countErr != nil {
			return execution.PurgeResult{}, countErr
		}
		result.Executions += count
	}
	return result, tx.Commit()
}

func (s *Store) CreatePipelineRun(ctx context.Context, r execution.CreatePipelineRun) (execution.PipelineRun, error) {
	now := time.Now().UTC()
	run := execution.PipelineRun{ID: uuid.New(), PipelineName: r.PipelineName, PipelineVersion: r.PipelineVersion, Input: r.Input, Status: execution.RunPending, IdempotencyKey: r.IdempotencyKey, CreatedAt: now, UpdatedAt: now}
	var idempotency any
	if r.IdempotencyKey != "" {
		idempotency = r.IdempotencyKey
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO pipeline_runs(id,pipeline_name,pipeline_version,input,status,idempotency_key,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$7) ON CONFLICT DO NOTHING`, run.ID, run.PipelineName, run.PipelineVersion, []byte(run.Input), run.Status, idempotency, now)
	if err != nil {
		return execution.PipelineRun{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected > 0 {
		return run, err
	}
	if r.IdempotencyKey == "" {
		return execution.PipelineRun{}, errors.New("pipeline insert conflicted without an idempotency key")
	}
	return scanPipelineRun(s.db.QueryRowContext(ctx, `SELECT id,pipeline_name,pipeline_version,input,status,error,idempotency_key,created_at,updated_at,finished_at FROM pipeline_runs WHERE pipeline_name=$1 AND idempotency_key=$2`, r.PipelineName, r.IdempotencyKey))
}
func (s *Store) GetPipelineRun(ctx context.Context, id uuid.UUID) (execution.PipelineRun, error) {
	run, err := scanPipelineRun(s.db.QueryRowContext(ctx, `SELECT id,pipeline_name,pipeline_version,input,status,error,idempotency_key,created_at,updated_at,finished_at FROM pipeline_runs WHERE id=$1`, id))
	return run, translateNotFound(err)
}
func (s *Store) ListPipelineRuns(ctx context.Context, filter execution.RunFilter) ([]execution.PipelineRun, error) {
	query := `SELECT id,pipeline_name,pipeline_version,input,status,error,idempotency_key,created_at,updated_at,finished_at FROM pipeline_runs`
	args := []any{}
	conditions := []string{}
	if len(filter.Statuses) > 0 {
		values := make([]string, len(filter.Statuses))
		for i, status := range filter.Statuses {
			values[i] = string(status)
		}
		args = append(args, values)
		conditions = append(conditions, fmt.Sprintf("status = ANY($%d)", len(args)))
	}
	if filter.Before != nil {
		args = append(args, filter.Before.CreatedAt, filter.Before.ID)
		conditions = append(conditions, fmt.Sprintf("(created_at,id)<($%d,$%d)", len(args)-1, len(args)))
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, " AND ")
	}
	query += ` ORDER BY created_at DESC,id DESC`
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []execution.PipelineRun{}
	for rows.Next() {
		run, scanErr := scanPipelineRun(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, run)
	}
	return items, rows.Err()
}
func (s *Store) SetPipelineRunStatus(ctx context.Context, id uuid.UUID, status execution.RunStatus, text string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE pipeline_runs SET status=$2,error=$3,updated_at=now(),finished_at=CASE WHEN $2 IN ('succeeded','failed','cancelled') THEN now() ELSE NULL END WHERE id=$1`, id, status, text)
	return foundResult(result, err)
}

type scanner interface{ Scan(...any) error }

func scanPipelineRun(row scanner) (execution.PipelineRun, error) {
	var run execution.PipelineRun
	var finished sql.NullTime
	var raw []byte
	var idempotency sql.NullString
	err := row.Scan(&run.ID, &run.PipelineName, &run.PipelineVersion, &raw, &run.Status, &run.Error, &idempotency, &run.CreatedAt, &run.UpdatedAt, &finished)
	run.Input = raw
	if idempotency.Valid {
		run.IdempotencyKey = idempotency.String
	}
	if finished.Valid {
		run.FinishedAt = &finished.Time
	}
	return run, err
}

func scanExecution(row scanner) (execution.Execution, error) {
	var item execution.Execution
	var input, output []byte
	var leaseToken, runID uuid.NullUUID
	var leaseUntil, started, finished sql.NullTime
	var idem sql.NullString
	err := row.Scan(&item.ID, &item.TaskName, &item.TaskVersion, &input, &output, &item.Status, &item.Attempt, &item.MaxAttempts, &item.AvailableAt, &item.LeaseOwner, &leaseToken, &leaseUntil, &item.LastError, &idem, &runID, &item.NodeKey, &item.CreatedAt, &started, &finished)
	item.Input = input
	item.Output = output
	if leaseToken.Valid {
		item.LeaseToken = leaseToken.UUID
	}
	if runID.Valid {
		item.PipelineRunID = &runID.UUID
	}
	if leaseUntil.Valid {
		item.LeaseUntil = leaseUntil.Time
	}
	if started.Valid {
		item.StartedAt = &started.Time
	}
	if finished.Valid {
		item.FinishedAt = &finished.Time
	}
	if idem.Valid {
		item.IdempotencyKey = idem.String
	}
	return item, err
}
func (s *Store) insertEvent(ctx context.Context, tx *sql.Tx, item execution.Execution, event execution.EventType, text string) error {
	executor := interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}(s.db)
	if tx != nil {
		executor = tx
	}
	_, err := executor.ExecContext(ctx, `INSERT INTO execution_events(id,execution_id,event_type,attempt,error,created_at)VALUES($1,$2,$3,$4,$5,$6)`, uuid.New(), item.ID, event, item.Attempt, text, time.Now().UTC())
	return err
}
func prefixedColumns(alias string) string {
	parts := strings.Split(executionColumns, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ",")
}
func translateNotFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return execution.ErrNotFound
	}
	return err
}
func ownedResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return execution.ErrLeaseLost
	}
	return nil
}
func foundResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return execution.ErrNotFound
	}
	return nil
}

var _ execution.Store = (*Store)(nil)
