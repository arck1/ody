# schedulor

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

app := schedulor.NewFxApp(schedulor.FxAppOptions{
  DB:           db,
  Logger:       logger,
  QueueBackend: backend,
  TaskExecutor: taskExec,
  ExecutorOptions: &schedulor.LqExecutorOptions{
    PoolingTimeout: 30 * time.Second,
    PoolingBatch:   1,
  },
  SchedulerOptions: &schedulor.LqSchedulerOptions{
    TasksRefreshEnabled: true,
    TasksRefreshTimeout: 30 * time.Minute,
  },
})
app.Run()
```

## Bash File Executor

`LqExecutor` собирается из интерфейсов: backend очереди + стратегия выполнения задач.

Для запуска задач через bash-команды из JSON-файла:

1. Загрузи стратегию:
`exec, err := NewBashFileTaskExecutorFromFile("/absolute/path/to/commands.json")`
2. Передай ее в `NewLqExecutor` вместе с queue backend:
`lq := NewLqExecutor(logger, backend, exec, options)`

Пример `commands.json`:

```json
{
  "tasks": {
    "cleanup_tmp": "rm -rf /tmp/app-cache/*",
    "sync_reports": "/app/scripts/sync_reports.sh"
  },
  "list": [
    {
      "task_name": "health_ping",
      "command": "curl -fsS http://localhost:8080/health"
    }
  ]
}
```

Для in-process выполнения используй:
`NewCodeTaskExecutor([]TaskHandler{...})`.
