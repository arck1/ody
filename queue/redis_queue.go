package queue

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const defaultRedisPrefix = "schedulor:{queue}:"

// RedisQueueOptions controls Redis keys, retry limits, and task leases.
type RedisQueueOptions struct {
	// Prefix namespaces all keys. Keep a shared {...} hash tag when using Redis Cluster.
	Prefix string
	// TaskMaxAttempts limits deliveries before a task is moved to the DLQ.
	TaskMaxAttempts int
	// TaskVisibility is the duration of one claimed-task lease.
	TaskVisibility time.Duration
}

// RedisQueue is an atomic Redis implementation of Backend.
type RedisQueue struct {
	client  redis.UniversalClient
	options RedisQueueOptions
	now     func() time.Time
}

var _ Backend = (*RedisQueue)(nil)

// NewRedisQueue creates a Redis-backed queue. The caller owns the client lifecycle.
func NewRedisQueue(client redis.UniversalClient, options RedisQueueOptions) (*RedisQueue, error) {
	if client == nil {
		return nil, errors.New("redis queue client is nil")
	}
	options.Prefix = strings.TrimSpace(options.Prefix)
	if options.Prefix == "" {
		options.Prefix = defaultRedisPrefix
	}
	if options.TaskMaxAttempts <= 0 {
		options.TaskMaxAttempts = 25
	}
	if options.TaskVisibility <= 0 {
		options.TaskVisibility = 60 * time.Second
	}
	return &RedisQueue{client: client, options: options, now: time.Now}, nil
}

func (q *RedisQueue) sequenceKey() string    { return q.options.Prefix + "sequence" }
func (q *RedisQueue) idempotencyKey() string { return q.options.Prefix + "idempotency" }
func (q *RedisQueue) taskKey(id int64) string {
	return q.options.Prefix + "task:" + strconv.FormatInt(id, 10)
}
func (q *RedisQueue) taskKeyPrefix() string { return q.options.Prefix + "task:" }
func (q *RedisQueue) dlqKeyPrefix() string  { return q.options.Prefix + "dlq:" }
func (q *RedisQueue) dlqIndexKey() string   { return q.options.Prefix + "dlq" }
func (q *RedisQueue) readyKey(taskName string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(taskName))
	return q.options.Prefix + "ready:" + encoded
}

// Enqueue stores a task and makes it visible at availableAt.
func (q *RedisQueue) Enqueue(
	ctx context.Context,
	taskName string,
	payload JSONPayload,
	availableAt time.Time,
	idemKey string,
) (*int64, error) {
	taskName = strings.TrimSpace(taskName)
	if taskName == "" {
		return nil, errors.New("redis queue task name is empty")
	}
	if availableAt.IsZero() {
		availableAt = q.now().UTC()
	}
	idempotencyField := ""
	if idemKey != "" {
		idempotencyField = taskName + "\x00" + idemKey
	}
	value, err := enqueueRedisScript.Run(ctx, q.client, []string{
		q.sequenceKey(), q.idempotencyKey(), q.readyKey(taskName),
	}, q.taskKeyPrefix(), taskName, string(payload), availableAt.UnixMilli(), q.options.TaskMaxAttempts, idempotencyField).Int64()
	if err != nil {
		return nil, fmt.Errorf("enqueue redis task: %w", err)
	}
	return &value, nil
}

// Claim atomically leases ready tasks matching one of the requested names.
func (q *RedisQueue) Claim(ctx context.Context, tasks []string, limit int) ([]Claimed, error) {
	if limit <= 0 {
		limit = 1
	}
	keys := []string{q.idempotencyKey(), q.dlqIndexKey()}
	seen := make(map[string]struct{}, len(tasks))
	for _, name := range tasks {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		key := q.readyKey(name)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	if len(keys) == 2 {
		return []Claimed{}, nil
	}
	token := uuid.New()
	values, err := claimRedisScript.Run(ctx, q.client, keys,
		q.visibilityMilliseconds(), token.String(), limit,
		q.taskKeyPrefix(), q.dlqKeyPrefix(), "lease expired after maximum attempts",
	).Slice()
	if err != nil {
		return nil, fmt.Errorf("claim redis tasks: %w", err)
	}
	const fieldsPerTask = 7
	if len(values)%fieldsPerTask != 0 {
		return nil, fmt.Errorf("claim redis tasks: invalid result length %d", len(values))
	}
	claimed := make([]Claimed, 0, len(values)/fieldsPerTask)
	for index := 0; index < len(values); index += fieldsPerTask {
		id, parseErr := redisInt64(values[index])
		if parseErr != nil {
			return nil, fmt.Errorf("claim redis task id: %w", parseErr)
		}
		attempts, parseErr := redisInt64(values[index+5])
		if parseErr != nil {
			return nil, fmt.Errorf("claim redis task attempts: %w", parseErr)
		}
		maxAttempts, parseErr := redisInt64(values[index+6])
		if parseErr != nil {
			return nil, fmt.Errorf("claim redis task max attempts: %w", parseErr)
		}
		reservedUntil, parseErr := redisInt64(values[index+4])
		if parseErr != nil {
			return nil, fmt.Errorf("claim redis task lease: %w", parseErr)
		}
		claimed = append(claimed, Claimed{
			TaskID:        id,
			TaskName:      redisString(values[index+1]),
			Payload:       JSONPayload(redisString(values[index+2])),
			LeaseToken:    token,
			ReservedUntil: time.UnixMilli(reservedUntil).UTC(),
			Attempts:      int(attempts),
			MaxAttempts:   int(maxAttempts),
		})
	}
	return claimed, nil
}

// StartHeartbeat extends a lease until cancellation or ownership loss.
func (q *RedisQueue) StartHeartbeat(ctx context.Context, taskID int64, leaseToken uuid.UUID, lost chan struct{}) {
	heartbeatInterval := q.options.TaskVisibility / 3
	if heartbeatInterval <= 0 {
		heartbeatInterval = q.options.TaskVisibility
	}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := q.Heartbeat(ctx, taskID, leaseToken)
			if err != nil && ctx.Err() != nil {
				return
			}
			if err != nil || !ok {
				notifyLeaseLost(lost)
				return
			}
		}
	}
}

// Heartbeat extends an unexpired lease owned by leaseToken.
func (q *RedisQueue) Heartbeat(ctx context.Context, taskID int64, leaseToken uuid.UUID) (bool, error) {
	updated, err := heartbeatRedisScript.Run(ctx, q.client, []string{q.taskKey(taskID)},
		leaseToken.String(), q.visibilityMilliseconds(), taskID).Int()
	if err != nil {
		return false, fmt.Errorf("heartbeat redis task: %w", err)
	}
	return updated == 1, nil
}

// Ack removes a successfully processed task when the lease token matches.
func (q *RedisQueue) Ack(ctx context.Context, taskID int64, leaseToken uuid.UUID) (bool, error) {
	removed, err := ackRedisScript.Run(ctx, q.client, []string{q.taskKey(taskID), q.idempotencyKey()},
		taskID, leaseToken.String()).Int()
	if err != nil {
		return false, fmt.Errorf("ack redis task: %w", err)
	}
	return removed == 1, nil
}

// Nack releases a task and schedules its next delivery.
func (q *RedisQueue) Nack(
	ctx context.Context,
	taskID int64,
	leaseToken uuid.UUID,
	errText string,
	delay time.Duration,
) (bool, error) {
	updated, err := nackRedisScript.Run(ctx, q.client, []string{q.taskKey(taskID)},
		taskID, leaseToken.String(), errText, delay.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("nack redis task: %w", err)
	}
	return updated == 1, nil
}

// MoveToDLQ atomically archives a failed task when the lease token matches.
func (q *RedisQueue) MoveToDLQ(ctx context.Context, taskID int64, leaseToken uuid.UUID, errText string) (bool, error) {
	moved, err := moveToDLQRedisScript.Run(ctx, q.client, []string{
		q.taskKey(taskID), q.dlqKeyPrefix() + strconv.FormatInt(taskID, 10), q.idempotencyKey(), q.dlqIndexKey(),
	}, taskID, leaseToken.String(), errText).Int()
	if err != nil {
		return false, fmt.Errorf("move redis task to dlq: %w", err)
	}
	return moved == 1, nil
}

func (q *RedisQueue) visibilityMilliseconds() int64 {
	milliseconds := q.options.TaskVisibility.Milliseconds()
	if milliseconds < 1 {
		return 1
	}
	return milliseconds
}

func notifyLeaseLost(lost chan struct{}) {
	select {
	case lost <- struct{}{}:
	default:
	}
}

func redisString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}

func redisInt64(value any) (int64, error) {
	return strconv.ParseInt(redisString(value), 10, 64)
}

var enqueueRedisScript = redis.NewScript(`
local taskPrefix = ARGV[1]
local idemField = ARGV[6]
if idemField ~= '' then
  local existing = redis.call('HGET', KEYS[2], idemField)
  if existing then
    if redis.call('EXISTS', taskPrefix .. existing) == 1 then
      return existing
    end
    redis.call('HDEL', KEYS[2], idemField)
  end
end
local id = redis.call('INCR', KEYS[1])
local taskKey = taskPrefix .. id
redis.call('HSET', taskKey,
  'task_name', ARGV[2],
  'payload', ARGV[3],
  'available_at', ARGV[4],
  'reserved_until', 0,
  'attempts', 0,
  'max_attempts', ARGV[5],
  'lease_token', '',
  'last_error', '',
  'ready_key', KEYS[3],
  'idempotency_field', idemField)
redis.call('ZADD', KEYS[3], ARGV[4], id)
if idemField ~= '' then
  redis.call('HSET', KEYS[2], idemField, id)
end
return id
`)

var claimRedisScript = redis.NewScript(`
redis.replicate_commands()
local serverTime = redis.call('TIME')
local now = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
local leaseUntil = now + tonumber(ARGV[1])
local token = ARGV[2]
local limit = tonumber(ARGV[3])
local taskPrefix = ARGV[4]
local dlqPrefix = ARGV[5]
local exhaustedError = ARGV[6]
local result = {}

local function archiveExhausted(taskKey, id, readyKey)
  local dlqKey = dlqPrefix .. id
  local values = redis.call('HGETALL', taskKey)
  for index = 1, #values, 2 do
    redis.call('HSET', dlqKey, values[index], values[index + 1])
  end
  local lastError = redis.call('HGET', taskKey, 'last_error')
  if not lastError or lastError == '' then
    lastError = exhaustedError
  end
  redis.call('HSET', dlqKey, 'last_error', lastError, 'failed_at', now)
  redis.call('ZADD', KEYS[2], now, id)
  redis.call('ZREM', readyKey, id)
  local idemField = redis.call('HGET', taskKey, 'idempotency_field')
  if idemField and idemField ~= '' and redis.call('HGET', KEYS[1], idemField) == id then
    redis.call('HDEL', KEYS[1], idemField)
  end
  redis.call('DEL', taskKey)
end

for claimed = 1, limit do
  local bestID = nil
  local bestScore = nil
  local bestReadyKey = nil
  for keyIndex = 3, #KEYS do
    local readyKey = KEYS[keyIndex]
    while true do
      local head = redis.call('ZRANGE', readyKey, 0, 0, 'WITHSCORES')
      if #head == 0 or tonumber(head[2]) > now then
        break
      end
      local id = head[1]
      local taskKey = taskPrefix .. id
      if redis.call('EXISTS', taskKey) == 0 then
        redis.call('ZREM', readyKey, id)
      else
        local attempts = tonumber(redis.call('HGET', taskKey, 'attempts') or '0')
        local maxAttempts = tonumber(redis.call('HGET', taskKey, 'max_attempts') or '1')
        if attempts >= maxAttempts then
          archiveExhausted(taskKey, id, readyKey)
        else
          local score = tonumber(head[2])
          if not bestScore or score < bestScore or (score == bestScore and tonumber(id) < tonumber(bestID)) then
            bestID = id
            bestScore = score
            bestReadyKey = readyKey
          end
          break
        end
      end
    end
  end
  if not bestID then
    break
  end
  local taskKey = taskPrefix .. bestID
  local attempts = redis.call('HINCRBY', taskKey, 'attempts', 1)
  redis.call('HSET', taskKey, 'lease_token', token, 'reserved_until', leaseUntil)
  redis.call('ZADD', bestReadyKey, leaseUntil, bestID)
  local values = redis.call('HMGET', taskKey, 'task_name', 'payload', 'max_attempts')
  table.insert(result, bestID)
  table.insert(result, values[1])
  table.insert(result, values[2])
  table.insert(result, token)
  table.insert(result, leaseUntil)
  table.insert(result, attempts)
  table.insert(result, values[3])
end
return result
`)

var heartbeatRedisScript = redis.NewScript(`
redis.replicate_commands()
local serverTime = redis.call('TIME')
local now = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
local token = redis.call('HGET', KEYS[1], 'lease_token')
local reservedUntil = tonumber(redis.call('HGET', KEYS[1], 'reserved_until') or '0')
if token ~= ARGV[1] or reservedUntil <= now then
  return 0
end
local readyKey = redis.call('HGET', KEYS[1], 'ready_key')
local leaseUntil = now + tonumber(ARGV[2])
redis.call('HSET', KEYS[1], 'reserved_until', leaseUntil)
redis.call('ZADD', readyKey, leaseUntil, ARGV[3])
return 1
`)

var ackRedisScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[2] then
  return 0
end
local readyKey = redis.call('HGET', KEYS[1], 'ready_key')
local idemField = redis.call('HGET', KEYS[1], 'idempotency_field')
redis.call('ZREM', readyKey, ARGV[1])
if idemField and idemField ~= '' and redis.call('HGET', KEYS[2], idemField) == ARGV[1] then
  redis.call('HDEL', KEYS[2], idemField)
end
redis.call('DEL', KEYS[1])
return 1
`)

var nackRedisScript = redis.NewScript(`
redis.replicate_commands()
if redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[2] then
  return 0
end
local readyKey = redis.call('HGET', KEYS[1], 'ready_key')
local serverTime = redis.call('TIME')
local now = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
local availableAt = now + tonumber(ARGV[4])
redis.call('HSET', KEYS[1],
  'last_error', ARGV[3],
  'available_at', availableAt,
  'reserved_until', 0,
  'lease_token', '')
redis.call('ZADD', readyKey, availableAt, ARGV[1])
return 1
`)

var moveToDLQRedisScript = redis.NewScript(`
redis.replicate_commands()
if redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[2] then
  return 0
end
local values = redis.call('HGETALL', KEYS[1])
for index = 1, #values, 2 do
  redis.call('HSET', KEYS[2], values[index], values[index + 1])
end
local serverTime = redis.call('TIME')
local now = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
redis.call('HSET', KEYS[2], 'last_error', ARGV[3], 'failed_at', now)
redis.call('ZADD', KEYS[4], now, ARGV[1])
local readyKey = redis.call('HGET', KEYS[1], 'ready_key')
local idemField = redis.call('HGET', KEYS[1], 'idempotency_field')
redis.call('ZREM', readyKey, ARGV[1])
if idemField and idemField ~= '' and redis.call('HGET', KEYS[3], idemField) == ARGV[1] then
  redis.call('HDEL', KEYS[3], idemField)
end
redis.call('DEL', KEYS[1])
return 1
`)
