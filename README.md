# schedulor

## Examples

Готовые сценарии запуска: `examples/README.md`.

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
- `--executor` или `SCHEDULOR_EXECUTOR` (`bash`)
- `--bash-commands-file` или `SCHEDULOR_BASH_COMMANDS_FILE`
- `--executor-task-names` или `SCHEDULOR_EXECUTOR_TASK_NAMES`
- `--executor-command-field` или `SCHEDULOR_EXECUTOR_COMMAND_FIELD`
- `--with-scheduler` или `SCHEDULOR_WITH_SCHEDULER` (`true`)
- `--log-level` или `SCHEDULOR_LOG_LEVEL` (`debug|info|warn|error`)

## Fx Entry Point

Для запуска через `fx` используй `NewFxApp(...)`:

```go
backend := queue.NewPostgresQueue(db, &queue.PostgresQueueOptions{
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

Запуск worker:

```go
runner, err := worker.New(store, registry, engine, observer, worker.Options{
  Concurrency:       8,
  PollInterval:     100 * time.Millisecond,
  LeaseDuration:    30 * time.Second,
  HeartbeatInterval: 10 * time.Second,
})
go runner.Run(ctx)
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
