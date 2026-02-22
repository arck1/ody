package queue

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/google/uuid"
	"gorm.io/datatypes"
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

// Enqueue: идемпотентность по (topic, idempotency_key) опционально
func (q *PostgresQueue) Enqueue(
	ctx context.Context,
	taskName string,
	payload datatypes.JSONType[map[string]any],
	availableAt time.Time,
	idemKey string,
) (*int64, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return nil, err
	}
	var taskId int64
	err = db.WithContext(ctx).Raw(`
        INSERT INTO lq_tasks (task_name, payload, available_at, max_attempts, idempotency_key)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (task_name, idempotency_key)
        WHERE idempotency_key IS NOT NULL
        DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key  -- NOOP
        RETURNING task_id
    `,
		taskName,
		payload,
		availableAt,
		q.options.TaskMaxAttempts,
		idemKey,
	).Scan(&taskId).Error
	if err != nil {
		return nil, err
	}
	return &taskId, nil
}

func (q *PostgresQueue) Claim(ctx context.Context, tasks []string, limit int) ([]Claimed, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1
	}
	visMs := q.options.TaskVisibility.Milliseconds()
	newLeaseToken := q.GetNewLeaseToken()
	return gorm.G[Claimed](db).Raw(
		`
		WITH c AS (
		  SELECT task_id
		  FROM lq_tasks
		  WHERE task_name in ?
			AND available_at <= now()
		    AND reserved_until <= now()
		    AND attempts < max_attempts
		  ORDER BY available_at, task_id
		  FOR UPDATE SKIP LOCKED
		  LIMIT ?
		)
		UPDATE lq_tasks q
		SET reserved_until = now() + (? * interval '1 millisecond'),
		    attempts       = q.attempts + 1,
		    lease_token    = ?
		FROM c
		WHERE q.task_id = c.task_id
		RETURNING q.task_id, q.task_name, q.payload, q.lease_token, q.reserved_until, q.attempts, q.max_attempts
		`,
		tasks,
		limit,
		visMs,
		newLeaseToken,
	).Find(ctx)
}

func (q *PostgresQueue) GetHeartbeatTicker() *time.Ticker {
	return time.NewTicker(q.options.TaskVisibility / 3)
}

// StartHeartbeat Запускает Heartbeat для задачи
func (q *PostgresQueue) StartHeartbeat(
	ctx context.Context,
	taskId int64,
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
			ok, err := q.Heartbeat(ctx, taskId, leaseToken)
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

// Heartbeat: продлить аренду, только если ещё наша и не просрочена
func (q *PostgresQueue) Heartbeat(
	ctx context.Context,
	taskId int64,
	leaseToken uuid.UUID,
) (bool, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return false, err
	}
	visMs := q.options.TaskVisibility.Milliseconds()
	result := gorm.WithResult()
	err = gorm.G[any](db, result).Exec(
		ctx,
		`
			UPDATE lq_tasks
			SET reserved_until = now() + (? * interval '1 millisecond')
			WHERE task_id = ? AND lease_token = ? AND reserved_until > now()
		`,
		visMs,
		taskId,
		leaseToken,
	)
	return result.RowsAffected > 0, err
}

// Ack: удалить, если аренда совпадает
func (q *PostgresQueue) Ack(ctx context.Context, taskId int64, leaseToken uuid.UUID) (bool, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return false, err
	}
	result := gorm.WithResult()
	err = gorm.G[any](db, result).Exec(
		ctx,
		`DELETE FROM lq_tasks WHERE task_id = ? AND lease_token = ? RETURNING true`,
		taskId,
		leaseToken,
	)
	return result.RowsAffected > 0, err
}

// Nack: вернуть в очередь с задержкой
func (q *PostgresQueue) Nack(
	ctx context.Context,
	taskId int64,
	leaseToken uuid.UUID,
	errText string,
	delay time.Duration,
) (bool, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return false, err
	}
	delayMs := delay.Milliseconds()

	result := gorm.WithResult()
	err = gorm.G[any](db, result).Exec(
		ctx,
		`
		UPDATE lq_tasks
		SET last_error = ?,
		    reserved_until = 'epoch',
		    available_at = now() + (? * interval '1 millisecond')
		WHERE task_id = ? AND lease_token = ?`,
		errText,
		delayMs,
		taskId,
		leaseToken,
	)
	return result.RowsAffected > 0, err
}

// MoveToDLQ: если попытки превышены — перенос
func (q *PostgresQueue) MoveToDLQ(ctx context.Context, taskId int64) (bool, error) {
	db, err := q.db.GetConnect(ctx)
	if err != nil {
		return false, err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		result := gorm.WithResult()
		err = gorm.G[any](tx, result).Exec(
			ctx,
			`INSERT INTO lq_tasks_dlq SELECT * FROM lq_tasks WHERE task_id = ? AND attempts >= max_attempts`,
			taskId,
		)
		if err != nil {
			return err
		}
		if result.RowsAffected < 1 {
			return nil
		}
		return gorm.G[any](tx).Exec(
			ctx,
			`DELETE FROM lq_tasks WHERE task_id = ?`,
			taskId,
		)
	})

	return err == nil, err
}
