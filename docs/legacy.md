# Cron и legacy queue

Этот API поддерживает `LqScheduler`, существующий `LqExecutor`, in-process/bash executors и queue
backends. Для новых workflow с историей и pipeline используйте typed architecture.

## PostgreSQL queue

Legacy API принимает небольшой connector вместо прямого `*sql.DB`:

```go
type DBConnector struct{ DB *sql.DB }

func (c DBConnector) GetConnect(context.Context) (*sql.DB, error) {
    return c.DB, nil
}
```

```go
backend := queue.NewPostgresQueue(dbConnector, queue.PostgresQueueOptions{
    TaskMaxAttempts: 25,
    TaskVisibility:  60 * time.Second,
})
```

Примените root `schema.sql`: он создаёт `lq_tasks`, `lq_tasks_dlq`, `lq_schedules` и leader state.

## Redis queue

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
defer client.Close()

backend, err := queue.NewRedisQueue(client, queue.RedisQueueOptions{
    Prefix:          "billing:{queue}:",
    TaskMaxAttempts: 5,
    TaskVisibility:  30 * time.Second,
})
```

Redis queue поддерживает delayed enqueue, idempotency, atomic claim, heartbeat, nack/redelivery и
DLQ. Для Redis Cluster prefix обязан содержать общий hash tag, чтобы Lua keys находились в одном
slot. После `Ack` активная запись и idempotency entry удаляются.

## Go handlers

```go
taskExecutor := schedulor.NewCodeTaskExecutor([]schedulor.TaskHandler{
    {
        TaskName: "billing.sync",
        Handler: func(ctx context.Context, payload map[string]any) error {
            return syncBilling(ctx, payload)
        },
    },
})

executor, err := schedulor.NewLqExecutor(logger, backend, taskExecutor,
    &schedulor.LqExecutorOptions{
        PoolingTimeout: 100 * time.Millisecond,
        PoolingBatch:   10,
    },
)
```

## Bash executor

```json
{
  "list": [
    {
      "task_name": "reports.build",
      "args": ["/app/bin/build-report", "--daily"]
    }
  ]
}
```

```go
commands, err := schedulor.LoadBashTaskCommandsFromFile(path)
taskExecutor := schedulor.NewBashTaskExecutorWithCommands(commands)
```

Команда запускается напрямую как binary + args, без `bash -lc`. Shell pipes, redirects и glob не
обрабатываются. Не передавайте непроверенный пользовательский ввод как executable/arguments.

## Cron schedules

`LqScheduler` читает таблицу `lq_schedules`. Основные поля: UUID `id`, `task_name`, cron expression,
JSON `payload`, `is_active`, `updated` и description. Изменение `updated` заставляет scheduler
перезагрузить job; удалённая или неактивная запись снимается с выполнения.

Cron expression использует стандартные пять полей без секунд:

```sql
INSERT INTO lq_schedules (task_name, cron, payload, is_active, description)
VALUES (
    'billing.sync',
    '*/10 * * * *',
    '{"account_id":"42"}'::jsonb,
    true,
    'Synchronize billing every ten minutes'
);
```

```go
scheduler, err := schedulor.NewLqScheduler(dbConnector, logger, executor,
    &schedulor.LqSchedulerOptions{
        TasksRefreshEnabled: true,
        TasksRefreshTimeout: 30 * time.Second,
    },
)
```

Для нескольких instances передайте PostgreSQL leader elector. `AddJob` выполняется только лидером,
а `AddLocalJob` — на каждом instance.

## Legacy CLI

PostgreSQL scheduler + bash commands:

```bash
SCHEDULOR_DB_DSN='postgres://schedulor:schedulor@localhost:5432/schedulor?sslmode=disable' \
SCHEDULOR_BASH_COMMANDS_FILE='/app/commands.json' \
go run ./cmd/schedulor
```

Redis worker без PostgreSQL scheduler:

```bash
SCHEDULOR_QUEUE_BACKEND=redis \
SCHEDULOR_REDIS_URL='redis://localhost:6379/0' \
SCHEDULOR_WITH_SCHEDULER=false \
go run ./cmd/schedulor
```

`queue.KafkaQueue` пока является placeholder и не должен выбираться в production.
