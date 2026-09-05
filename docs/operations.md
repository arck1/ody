# Запуск и эксплуатация

## Standalone

`schedulor.App.Run(ctx)` блокируется до отмены контекста:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

if err := app.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    return err
}
```

Один worker instance может иметь несколько polling loops через `Concurrency`. Для горизонтального
масштабирования запускайте несколько процессов с разными `Options.ID`.

## Uber Fx

Fx создаёт тот же `schedulor.App`, который используется при standalone-запуске. Store предоставьте
как интерфейс `execution.Store`:

```go
app := fx.New(
    fx.Provide(
        fx.Annotate(newStore, fx.As(new(execution.Store))),
    ),
    schedulor.FxModule(
        schedulor.Tasks(emailModule),
        schedulor.Pipelines(importPipeline),
        schedulor.Observe(observer),
        schedulor.WithWorker(options),
    ),
)
app.Run()
```

Fx module отменяет worker context на shutdown и ожидает остановки polling loops.

## Prometheus

```go
registry := prometheus.NewRegistry()
metrics, err := monitoringprom.New(registry, store)
runner, err := worker.New(store, tasks, engine, metrics, options)

metricsHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
```

Экспортируются transitions, handler duration, observer errors и текущие количества executions и
pipeline runs по status. Не добавляйте execution ID, error text или пользовательские значения в
labels: это создаёт высокую cardinality.

## Operational CLI

```bash
export SCHEDULOR_DB_DSN='postgres://schedulor:schedulor@localhost:5432/schedulor?sslmode=disable'

go run ./cmd/schedulor-admin tasks -status running -limit 100
go run ./cmd/schedulor-admin tasks -name email.send -status failed
go run ./cmd/schedulor-admin task '<execution-uuid>'
go run ./cmd/schedulor-admin pipelines -status running,failed -limit 100
go run ./cmd/schedulor-admin pipeline '<pipeline-uuid>'
go run ./cmd/schedulor-admin restart '<execution-uuid>'
go run ./cmd/schedulor-admin cancel '<execution-uuid>' 'requested by customer'
go run ./cmd/schedulor-admin purge -older-than 720h -limit 1000
```

CLI и web UI могут работать с Redis без изменения команд:

```bash
export SCHEDULOR_STORE=redis
export SCHEDULOR_REDIS_URL='redis://127.0.0.1:6379/0'
export SCHEDULOR_REDIS_PREFIX='myapp:{execution}:'

go run ./cmd/schedulor-admin tasks -status running
go run ./cmd/schedulor-admin serve
```

Вывод CLI — JSON, поэтому его можно передавать в `jq`.

`purge` удаляет только terminal standalone executions и terminal pipeline runs вместе с их узлами,
событиями и idempotency references. Запускайте его периодически небольшими batch; активные задачи,
blocked descendants и незавершённые pipelines не удаляются.

HTTP-списки ограничены 100 элементами по умолчанию. Для следующей страницы передайте
`before_time=<created_at>&before_id=<id>` последнего элемента предыдущей страницы. Максимальный
`limit` для web API — 1000. Go API использует `execution.Cursor` в `ListFilter.Before` и
`RunFilter.Before`.

## Web UI и JSON API

```bash
go run ./cmd/schedulor-admin --addr 127.0.0.1:8081 serve
```

- dashboard: `GET /`;
- Prometheus: `GET /metrics`;
- задачи: `GET /api/tasks`, `GET /api/tasks/{id}`;
- управление: `POST /api/tasks/{id}/restart`, `POST /api/tasks/{id}/cancel`;
- pipelines: `GET /api/pipelines`, `GET /api/pipelines/{id}`.

Web server по умолчанию слушает loopback и не содержит authentication. Для внешнего доступа
используйте TLS reverse proxy с authentication/authorization. Input, output и errors могут содержать
чувствительные данные.

## Production checklist

- применяйте PostgreSQL migration до запуска worker либо выделите Redis DB/prefix;
- настройте pool `*sql.DB` или Redis client pool и timeout;
- для Redis включите `noeviction`, persistence, replication и backup;
- обеспечьте graceful shutdown и context-aware handlers;
- используйте идемпотентные side effects;
- оставляйте старые task/pipeline versions зарегистрированными до завершения старых записей;
- собирайте `/metrics` и alert на рост `failed`, `retry`, `scrape_error`;
- ограничьте доступ к operator API;
- периодически определяйте retention/архивацию terminal executions и events на уровне приложения;
- запускайте `make test-functional-integration` с реальными PostgreSQL и Redis перед релизом.
