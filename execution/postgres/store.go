// Package postgres implements execution.Store with PostgreSQL.
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

type Store struct{ db *sql.DB }

func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, errors.New("postgres execution store db is nil")
	}
	return &Store{db: db}, nil
}

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
	defer tx.Rollback()
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
	query := `SELECT ` + executionColumns + ` FROM task_executions WHERE ` + strings.Join(conditions, " AND ") + ` ORDER BY created_at DESC,id`
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

func (s *Store) Claim(ctx context.Context, owner string, names []string, limit int, lease time.Duration) ([]execution.Execution, error) {
	if limit <= 0 {
		limit = 1
	}
	token := uuid.New()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `WITH candidates AS (
        SELECT id FROM task_executions
        WHERE task_name = ANY($1) AND available_at <= now()
          AND ((status IN ('pending','retry')) OR (status='running' AND lease_until <= now()))
          AND attempt < max_attempts
        ORDER BY available_at,created_at FOR UPDATE SKIP LOCKED LIMIT $2)
      UPDATE task_executions e SET status='running', attempt=e.attempt+1, lease_owner=$3,
        lease_token=$4, lease_until=now()+($5*interval '1 millisecond'), started_at=COALESCE(started_at,now())
      FROM candidates c WHERE e.id=c.id RETURNING `+prefixedColumns("e"), names, limit, owner, token, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
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
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+executionColumns+` FROM task_executions WHERE status='running' AND lease_until<=now() AND attempt>=max_attempts FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return nil, err
	}
	items := []execution.Execution{}
	for rows.Next() {
		item, scanErr := scanExecution(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		items = append(items, item)
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
func (s *Store) Retry(ctx context.Context, id, token uuid.UUID, text string, at time.Time) error {
	return s.transition(ctx, id, token, execution.StatusRetry, execution.EventRetried, text, nil, at)
}
func (s *Store) Fail(ctx context.Context, id, token uuid.UUID, text string) error {
	return s.transition(ctx, id, token, execution.StatusFailed, execution.EventFailed, text, nil, time.Time{})
}

func (s *Store) transition(ctx context.Context, id, token uuid.UUID, status execution.Status, event execution.EventType, text string, output json.RawMessage, available time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
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
	defer tx.Rollback()
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

func (s *Store) CreatePipelineRun(ctx context.Context, r execution.CreatePipelineRun) (execution.PipelineRun, error) {
	now := time.Now().UTC()
	run := execution.PipelineRun{ID: uuid.New(), PipelineName: r.PipelineName, PipelineVersion: r.PipelineVersion, Input: r.Input, Status: execution.RunPending, CreatedAt: now, UpdatedAt: now}
	_, err := s.db.ExecContext(ctx, `INSERT INTO pipeline_runs(id,pipeline_name,pipeline_version,input,status,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$6)`, run.ID, run.PipelineName, run.PipelineVersion, []byte(run.Input), run.Status, now)
	return run, err
}
func (s *Store) GetPipelineRun(ctx context.Context, id uuid.UUID) (execution.PipelineRun, error) {
	var run execution.PipelineRun
	var finished sql.NullTime
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT id,pipeline_name,pipeline_version,input,status,error,created_at,updated_at,finished_at FROM pipeline_runs WHERE id=$1`, id).Scan(&run.ID, &run.PipelineName, &run.PipelineVersion, &raw, &run.Status, &run.Error, &run.CreatedAt, &run.UpdatedAt, &finished)
	run.Input = raw
	if finished.Valid {
		run.FinishedAt = &finished.Time
	}
	return run, translateNotFound(err)
}
func (s *Store) ListPipelineRuns(ctx context.Context, statuses []execution.RunStatus) ([]execution.PipelineRun, error) {
	query := `SELECT id,pipeline_name,pipeline_version,input,status,error,created_at,updated_at,finished_at FROM pipeline_runs`
	args := []any{}
	if len(statuses) > 0 {
		values := make([]string, len(statuses))
		for i, status := range statuses {
			values[i] = string(status)
		}
		query += ` WHERE status = ANY($1)`
		args = append(args, values)
	}
	query += ` ORDER BY created_at DESC,id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []execution.PipelineRun{}
	for rows.Next() {
		var run execution.PipelineRun
		var raw []byte
		var finished sql.NullTime
		if err = rows.Scan(&run.ID, &run.PipelineName, &run.PipelineVersion, &raw, &run.Status, &run.Error, &run.CreatedAt, &run.UpdatedAt, &finished); err != nil {
			return nil, err
		}
		run.Input = raw
		if finished.Valid {
			run.FinishedAt = &finished.Time
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
