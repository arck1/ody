# schedulor

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
- `--executor` или `SCHEDULOR_EXECUTOR` (`bash_file|bash_payload`)
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
taskExec, err := schedulor.NewBashFileTaskExecutorFromFile("/absolute/path/to/commands.json")
if err != nil {
  panic(err)
}

if err := schedulor.LoadSettingsFromEnv(); err != nil {
  panic(err)
}

executor, err := schedulor.NewLqExecutor(logger, backend, taskExec, &schedulor.LqExecutorOptions{
  PoolingTimeout: 30 * time.Second,
  PoolingBatch:   1,
})
if err != nil {
  panic(err)
}
scheduler, err := schedulor.NewLqScheduler(db, logger, executor, &schedulor.LqSchedulerOptions{
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

## Bash File Executor

`LqExecutor` собирается из интерфейсов: backend очереди + стратегия выполнения задач.

Для запуска задач через bash-команды из JSON-файла:

1. Загрузи стратегию:
`exec, err := NewBashFileTaskExecutorFromFile("/absolute/path/to/commands.json")`
2. Передай ее в `NewLqExecutor` вместе с queue backend:
`lq, err := NewLqExecutor(logger, backend, exec, options)`

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
