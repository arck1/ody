package queue

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
)

type PostgresQueue struct {
	db      DbConnector
	options PostgresQueueOptions
}

func NewPostgresQueue(db DbConnector, options *PostgresQueueOptions) *PostgresQueue {
	resolved := PostgresQueueOptions{
		TaskMaxAttempts: 25,
		TaskVisibility:  60 * time.Second,
	}
	if options != nil {
		if options.TaskMaxAttempts > 0 {
			resolved.TaskMaxAttempts = options.TaskMaxAttempts
		}
		if options.TaskVisibility > 0 {
			resolved.TaskVisibility = options.TaskVisibility
		}
	}
	return &PostgresQueue{
		db:      db,
		options: resolved,
	}
}

func (q *PostgresQueue) GetNewLeaseToken() uuid.UUID {
	return uuid.New()
}

func (q *PostgresQueue) Enqueue(
	ctx context.Context,
	taskName string,
	payload JSONPayload,
	availableAt time.Time,
	idemKey string,
) (*int64, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return nil, err
	}
	var taskID int64
	var idem any
	if idemKey != "" {
		idem = idemKey
	}
	err = db.QueryRowContext(
		ctx,
		`INSERT INTO lq_tasks (task_name, payload, available_at, max_attempts, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (task_name, idempotency_key)
		 WHERE idempotency_key IS NOT NULL
		 DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
		 RETURNING task_id`,
		taskName,
		[]byte(payload),
		availableAt,
		q.options.TaskMaxAttempts,
		idem,
	).Scan(&taskID)
	if err != nil {
		return nil, err
	}
	return &taskID, nil
}

func (q *PostgresQueue) Claim(ctx context.Context, tasks []string, limit int) ([]Claimed, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1
	}
	visMS := q.options.TaskVisibility.Milliseconds()
	newLeaseToken := q.GetNewLeaseToken()

	rows, err := db.QueryContext(
		ctx,
		`WITH c AS (
			SELECT task_id
			FROM lq_tasks
			WHERE task_name = ANY($1)
			  AND available_at <= now()
			  AND reserved_until <= now()
			  AND attempts < max_attempts
			ORDER BY available_at, task_id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE lq_tasks q
		SET reserved_until = now() + ($3 * interval '1 millisecond'),
		    attempts       = q.attempts + 1,
		    lease_token    = $4
		FROM c
		WHERE q.task_id = c.task_id
		RETURNING q.task_id, q.task_name, q.payload, q.lease_token, q.reserved_until, q.attempts, q.max_attempts`,
		tasks,
		limit,
		visMS,
		newLeaseToken,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	claimed := make([]Claimed, 0, limit)
	for rows.Next() {
		var item Claimed
		var payloadRaw []byte
		if scanErr := rows.Scan(
			&item.TaskID,
			&item.TaskName,
			&payloadRaw,
			&item.LeaseToken,
			&item.ReservedUntil,
			&item.Attempts,
			&item.MaxAttempts,
		); scanErr != nil {
			return nil, scanErr
		}
		item.Payload = JSONPayload(payloadRaw)
		claimed = append(claimed, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return claimed, nil
}

func (q *PostgresQueue) GetHeartbeatTicker() *time.Ticker {
	return time.NewTicker(q.options.TaskVisibility / 3)
}

func (q *PostgresQueue) StartHeartbeat(
	ctx context.Context,
	taskID int64,
	leaseToken uuid.UUID,
	lost chan struct{},
) {
	ticker := q.GetHeartbeatTicker()
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := q.Heartbeat(ctx, taskID, leaseToken)
			if err != nil || !ok {
				select {
				case lost <- struct{}{}:
				default:
				}
				return
			}
		}
	}
}

func (q *PostgresQueue) Heartbeat(
	ctx context.Context,
	taskID int64,
	leaseToken uuid.UUID,
) (bool, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return false, err
	}
	visMS := q.options.TaskVisibility.Milliseconds()
	result, err := db.ExecContext(
		ctx,
		`UPDATE lq_tasks
		 SET reserved_until = now() + ($1 * interval '1 millisecond')
		 WHERE task_id = $2 AND lease_token = $3 AND reserved_until > now()`,
		visMS,
		taskID,
		leaseToken,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (q *PostgresQueue) Ack(ctx context.Context, taskID int64, leaseToken uuid.UUID) (bool, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return false, err
	}
	result, err := db.ExecContext(
		ctx,
		`DELETE FROM lq_tasks WHERE task_id = $1 AND lease_token = $2`,
		taskID,
		leaseToken,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (q *PostgresQueue) Nack(
	ctx context.Context,
	taskID int64,
	leaseToken uuid.UUID,
	errText string,
	delay time.Duration,
) (bool, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return false, err
	}
	delayMS := delay.Milliseconds()
	result, err := db.ExecContext(
		ctx,
		`UPDATE lq_tasks
		 SET last_error = $1,
		     reserved_until = 'epoch',
		     available_at = now() + ($2 * interval '1 millisecond')
		 WHERE task_id = $3 AND lease_token = $4`,
		errText,
		delayMS,
		taskID,
		leaseToken,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (q *PostgresQueue) MoveToDLQ(ctx context.Context, taskID int64) (bool, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return false, err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	insertRes, err := tx.ExecContext(
		ctx,
		`INSERT INTO lq_tasks_dlq SELECT * FROM lq_tasks WHERE task_id = $1 AND attempts >= max_attempts`,
		taskID,
	)
	if err != nil {
		return false, err
	}
	inserted, err := insertRes.RowsAffected()
	if err != nil {
		return false, err
	}
	if inserted < 1 {
		if err = tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}

	if _, err = tx.ExecContext(ctx, `DELETE FROM lq_tasks WHERE task_id = $1`, taskID); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
