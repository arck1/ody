# schedulor

## Документация

Полное пользовательское руководство: [docs/README.md](docs/README.md).

- [Быстрый старт](docs/getting-started.md)
- [Конфигурация](docs/configuration.md)
- [Generic-задачи](docs/tasks.md)
- [Пайплайны](docs/pipelines.md)
- [Запуск и эксплуатация](docs/operations.md)
- [Cron и legacy queue](docs/legacy.md)

## Examples

Готовые сценарии запуска: `examples/README.md`.

## Code quality

Проект использует зафиксированную версию `golangci-lint`. Локальная установка выполняется
автоматически в игнорируемый каталог `bin`:

```bash
make fmt        # применить gofmt и goimports
make fmt-check  # проверить форматирование без изменения файлов
make lint       # запустить линтеры
make lint-fix   # применить безопасные автоматические исправления
make check      # форматирование + линтеры + unit-тесты
```

## CLI

Запуск через CLI:

```bash
SCHEDULOR_DB_DSN='postgres://schedulor:schedulor@localhost:5432/schedulor?sslmode=disable' \
SCHEDULOR_BASH_COMMANDS_FILE='/absolute/path/to/commands.json' \
go run ./cmd/schedulor
```

Параметры CLI:

- `--queue-backend` или `SCHEDULOR_QUEUE_BACKEND` (`postgres|redis|kafka|noop`)
- `--db-dsn` или `SCHEDULOR_DB_DSN`
- `--redis-url` или `SCHEDULOR_REDIS_URL` (по умолчанию `redis://localhost:6379/0`)
- `--redis-prefix` или `SCHEDULOR_REDIS_PREFIX` (по умолчанию `schedulor:{queue}:`)
- `--executor` или `SCHEDULOR_EXECUTOR` (`bash`)
- `--bash-commands-file` или `SCHEDULOR_BASH_COMMANDS_FILE`
- `--executor-task-names` или `SCHEDULOR_EXECUTOR_TASK_NAMES`
- `--executor-command-field` или `SCHEDULOR_EXECUTOR_COMMAND_FIELD`
- `--with-scheduler` или `SCHEDULOR_WITH_SCHEDULER` (`true`)
- `--log-level` или `SCHEDULOR_LOG_LEVEL` (`debug|info|warn|error`)

Запуск worker с Redis без PostgreSQL scheduler:

```bash
SCHEDULOR_QUEUE_BACKEND=redis \
SCHEDULOR_REDIS_URL='redis://localhost:6379/0' \
SCHEDULOR_WITH_SCHEDULER=false \
go run ./cmd/schedulor
```

Redis backend реализует идемпотентную постановку, отложенную доставку, атомарный claim,
lease/heartbeat, `ack`, повторную доставку через `nack` и DLQ. Операции выполняются Lua-скриптами,
поэтому переходы состояния атомарны. Для Redis Cluster все ключи должны попадать в один hash slot:
сохраняйте общий hash tag (например, `{queue}`) в пользовательском prefix.

Программное подключение не привязано к конкретному режиму клиента go-redis:

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
backend, err := queue.NewRedisQueue(client, queue.RedisQueueOptions{
  Prefix:          "billing:{queue}:",
  TaskMaxAttempts: 5,
  TaskVisibility:  30 * time.Second,
})
if err != nil {
  return err
}
defer client.Close()
```

Клиент создаёт и закрывает приложение; queue использует переданный `redis.UniversalClient`.

## Fx Entry Point

Для запуска через `fx` используй `NewFxApp(...)`:

```go
backend := queue.NewPostgresQueue(db, queue.PostgresQueueOptions{
  TaskMaxAttempts: 25,
  TaskVisibility:  60 * time.Second,
})
commands := map[string]schedulor.BashTaskCommand{
  "health_ping": {Args: []string{"curl", "-fsS", "http://localhost:8080/health"}},
}
taskExec := schedulor.NewBashTaskExecutorWithCommands(commands)

if err := schedulor.LoadSettingsFromEnv(); err != nil {
  panic(err)
}

libraryLogger := schedulor.NewZapLogger(logger)

executor, err := schedulor.NewLqExecutor(libraryLogger, backend, taskExec, &schedulor.LqExecutorOptions{
  PoolingTimeout: 30 * time.Second,
  PoolingBatch:   1,
})
if err != nil {
  panic(err)
}
scheduler, err := schedulor.NewLqScheduler(db, libraryLogger, executor, &schedulor.LqSchedulerOptions{
  TasksRefreshEnabled: true,
  TasksRefreshTimeout: 30 * time.Minute,
})
if err != nil {
  panic(err)
}

app, err := schedulor.NewFxApp(schedulor.FxAppOptions{
  Components: []schedulor.FxLifecycleComponent{executor, scheduler},
})
if err != nil {
  panic(err)
}
app.Run()
```

`FxApp` больше не требует PostgreSQL по умолчанию: можно передать только нужные компоненты.

### Logging

Ядро зависит только от интерфейса `schedulor.Logger`:

```go
type Logger interface {
  Debug(message string, fields ...any)
  Info(message string, fields ...any)
  Warn(message string, fields ...any)
  Error(message string, fields ...any)
}
```

Поля передаются парами `key, value`. Для zap доступен готовый адаптер:

```go
logger := schedulor.NewZapLogger(zapLogger.Sugar())
executor, err := schedulor.NewLqExecutor(logger, backend, taskExec, options)
```

Можно передать собственную реализацию интерфейса без зависимости приложения от zap.

## Execution Engine and Typed Pipelines

Новая архитектура отделена от legacy `LqExecutor`. Она состоит из пакетов:

- `execution` — состояния, события и Store;
- `task` — типизированные `Definition[Input, Output]` и модули;
- `pipeline` — persistent DAG с передачей сохранённых результатов;
- `worker` — конкурентное выполнение, lease heartbeat, timeout и retries;
- `execution/postgres` — PostgreSQL Store и встроенная миграция.

Описание и обработчик задачи:

```go
Fetch := task.New[FetchInput, FetchOutput](
  "document.fetch",
  task.WithVersion(1),
  task.WithMaxAttempts(5),
  task.WithTimeout(30*time.Second),
)

module, err := task.NewModule("documents",
  task.Handle(Fetch, func(ctx context.Context, message task.Message[FetchInput]) (FetchOutput, error) {
    return fetch(ctx, message.Input)
  }),
)
registry, err := task.NewRegistry(module)
```

Самостоятельную задачу можно сразу сохранить:

```go
created, err := Fetch.Enqueue(ctx, store, FetchInput{URL: url},
  task.WithIdempotencyKey("fetch:"+url),
)
```

Pipeline строится типобезопасно. Результат каждого узла сохраняется в Store и только затем декодируется
для следующего узла:

```go
flow := pipeline.New[ImportInput]("document-import", 1)

fetched := pipeline.Start(flow, "fetch", Fetch,
  func(input ImportInput) FetchInput {
    return FetchInput{URL: input.URL}
  },
)

parsed := pipeline.Then(flow, fetched, "parse", Parse,
  func(output FetchOutput) ParseInput {
    return ParseInput{Content: output.Content}
  },
)

indexed := pipeline.Join2(flow, parsed, metadata, "index", Index,
  func(document ParsedDocument, meta Metadata) IndexInput {
    return IndexInput{Document: document, Metadata: meta}
  },
)

pipelines, err := pipeline.NewRegistry(flow)
engine, err := pipeline.NewEngine(store, pipelines)
run, err := pipeline.Run(ctx, engine, flow, ImportInput{URL: url})
```

Запуск worker без Fx — `Run` блокируется до отмены контекста и перед возвратом
дожидается polling goroutine. Handlers должны самостоятельно реагировать на отмену контекста:

```go
runner, err := worker.New(store, registry, engine, observer, worker.Options{
  Concurrency:       8,
  PollInterval:     100 * time.Millisecond,
  LeaseDuration:    30 * time.Second,
  HeartbeatInterval: 10 * time.Second,
})
if err != nil {
  return err
}
if err = runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
  return err
}
```

Для Fx используется отдельный опциональный адаптер `worker/fx`; пакет `worker`
от Fx не зависит:

```go
runner, err := worker.New(store, registry, engine, observer, workerOptions)
if err != nil {
  return err
}
lifecycle, err := workerfx.New(runner)
if err != nil {
  return err
}

app, err := schedulor.NewFxApp(schedulor.FxAppOptions{
  Components: []schedulor.FxLifecycleComponent{lifecycle},
  Options: []fx.Option{
    fx.Provide(provideApplicationDependencies),
  },
})
if err != nil {
  return err
}
app.Run()
```

Отслеживание выполнения:

```go
item, err := store.GetExecution(ctx, created.ID)
events, err := store.Events(ctx, created.ID)
snapshot, err := engine.Inspect(ctx, run.ID)
result, err := pipeline.Output(ctx, store, run.ID, indexed)
```

`task.Permanent(err)` завершает задачу без retry. `task.RetryAfter(err, delay)` задаёт задержку
конкретной попытки. `engine.Cancel` отменяет незавершённые узлы pipeline, а `engine.Reconcile`
восстанавливает продвижение DAG после остановки процесса между сохранением результата и созданием successor.

PostgreSQL Store:

```go
store, err := executionpostgres.New(db)
if err != nil {
  return err
}
if err = store.Migrate(ctx); err != nil {
  return err
}
```

Таблицы `task_executions`, `execution_events` и `pipeline_runs` содержат входы, результаты,
попытки, ошибки, lease и полную историю переходов.

## Monitoring, Prometheus и управление

Пакет `monitoring` предоставляет транспорт-независимые интерфейсы `TaskReader`,
`PipelineReader`, `Controller` и объединённый `API`. Базовый `Service` работает с любым
`execution.Store`. Перезапуск разрешён только для `succeeded`, `failed` и `cancelled` задач:
он сохраняет ID и историю, добавляет событие `restarted`, сбрасывает attempt/result/lease и
возвращает задачу в `pending`. Связанный pipeline снова становится `running`.

Prometheus observer и store collector подключаются к тому же registry:

```go
promRegistry := prometheus.NewRegistry()
metrics, err := monitoringprom.New(promRegistry, store)
if err != nil {
  return err
}

runner, err := worker.New(store, tasks, engine, metrics, workerOptions)
metricsHandler := promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{})
```

Доступные метрики:

- `schedulor_worker_task_transitions_total{task,status}`;
- `schedulor_worker_task_duration_seconds{task,status}`;
- `schedulor_worker_observer_errors_total{task}`;
- `schedulor_store_task_executions{task,status}`;
- `schedulor_store_pipeline_runs{pipeline,status}`;
- `schedulor_store_scrape_error`.

Operational CLI использует PostgreSQL execution Store:

```bash
export SCHEDULOR_DB_DSN='postgres://schedulor:schedulor@localhost:5432/schedulor?sslmode=disable'

go run ./cmd/schedulor-admin tasks -status failed -limit 50
go run ./cmd/schedulor-admin task '<execution-uuid>'
go run ./cmd/schedulor-admin pipelines running,failed
go run ./cmd/schedulor-admin pipeline '<pipeline-uuid>'
go run ./cmd/schedulor-admin restart '<execution-uuid>'
go run ./cmd/schedulor-admin cancel '<execution-uuid>' 'operator reason'
```

Web UI и JSON API запускаются отдельно от worker:

```bash
go run ./cmd/schedulor-admin --addr 127.0.0.1:8081 serve
```

Dashboard доступен на `http://127.0.0.1:8081/`, метрики — на `/metrics`, JSON API — под
`/api/tasks` и `/api/pipelines`. По умолчанию сервер слушает только loopback. При публикации
наружу добавьте authentication/TLS на reverse proxy и не открывайте operator endpoints напрямую.

### Functional tests

Функциональные сценарии используют только публичный API и MemoryStore. Каждый сценарий
оформлен отдельным `testify/suite`:

```bash
go test -count=1 -v ./functional
```

Покрыты standalone-выполнение и idempotency, отложенный запуск и retry, fan-out/fan-in pipeline,
остановка pipeline при permanent error, отмена и запуск worker через Fx lifecycle.

Отдельные infrastructure suites поднимают настоящие PostgreSQL 16 и Redis 7.4 через
Testcontainers и проверяют публичный API библиотеки вместе с сетевым протоколом, SQL и Lua:

```bash
make test-functional-integration
```

PostgreSQL suite проверяет generic task worker, durable input/output/events, idempotency,
Prometheus observer, operator restart и persistent fan-out/fan-in pipeline. Redis suite проверяет
delayed delivery, atomic claim, lease heartbeat, nack/redelivery, idempotency, DLQ и полный цикл
legacy `LqExecutor` до `ack`. Контейнеры изолированы, очищаются автоматически и требуют работающий
Docker daemon. Те же suites запускаются отдельным integration job в GitHub Actions.

## Bash Task Executor

`LqExecutor` собирается из интерфейсов: backend очереди + стратегия выполнения задач.

Для запуска задач через команды из JSON-файла:

1. На уровне конфигурации загрузить команды из файла через `schedulor.LoadBashTaskCommandsFromFile(...)`.
2. Передай ее в `NewLqExecutor` вместе с queue backend:
`exec := NewBashTaskExecutorWithCommands(commands)`

Пример `commands.json`:

```json
{
  "tasks": {
    "cleanup_tmp": "rm -rf /tmp/app-cache",
    "sync_reports": "/app/scripts/sync_reports.sh"
  },
  "list": [
    {
      "task_name": "health_ping",
      "command": "curl -fsS http://localhost:8080/health"
    },
    {
      "task_name": "health_ping_args",
      "args": ["curl", "-fsS", "http://localhost:8080/health"]
    }
  ]
}
```

Команды теперь выполняются напрямую (без `bash -lc`), как `binary + args`.
Shell-пайпы, редиректы и glob-расширения не поддерживаются.

Для in-process выполнения используй:
`NewCodeTaskExecutor([]TaskHandler{...})`.
