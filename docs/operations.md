# Запуск и эксплуатация

## Standalone

`worker.Run(ctx)` блокируется до отмены контекста:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    return err
}
```

Один worker instance может иметь несколько polling loops через `Concurrency`. Для горизонтального
масштабирования запускайте несколько процессов с разными `Options.ID`.

## Uber Fx

Core worker не зависит от Fx. Подключите adapter:

```go
runner, err := worker.New(store, tasks, engine, observer, options)
lifecycle, err := workerfx.New(runner)

app, err := schedulor.NewFxApp(schedulor.FxAppOptions{
    Components: []schedulor.FxLifecycleComponent{lifecycle},
    Options: []fx.Option{
        fx.Provide(provideApplicationDependencies),
    },
})
app.Run()
```

Fx adapter отменяет worker context на shutdown и ожидает остановки polling loops.

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
go run ./cmd/schedulor-admin pipelines running,failed
go run ./cmd/schedulor-admin pipeline '<pipeline-uuid>'
go run ./cmd/schedulor-admin restart '<execution-uuid>'
go run ./cmd/schedulor-admin cancel '<execution-uuid>' 'requested by customer'
```

Вывод CLI — JSON, поэтому его можно передавать в `jq`.

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

- применяйте PostgreSQL migration до запуска worker;
- настройте pool `*sql.DB` и лимиты PostgreSQL;
- обеспечьте graceful shutdown и context-aware handlers;
- используйте идемпотентные side effects;
- оставляйте старые task/pipeline versions зарегистрированными до завершения старых записей;
- собирайте `/metrics` и alert на рост `failed`, `retry`, `scrape_error`;
- ограничьте доступ к operator API;
- периодически определяйте retention/архивацию terminal executions и events на уровне приложения;
- запускайте `make test-functional-integration` с реальным PostgreSQL перед релизом.
