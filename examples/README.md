# Examples

Ниже набор рабочих сценариев запуска `schedulor`.

## 0. Подготовка окружения

1. Подними зависимости:
```bash
make up
```

2. Примени схему:
```bash
psql 'postgres://schedulor:schedulor@localhost:5432/schedulor?sslmode=disable' -f schema.sql
```

## 1. CLI + PostgreSQL + команды из файла

Используется JSON-конфиг: `examples/commands/basic.json`.

```bash
SCHEDULOR_QUEUE_BACKEND=postgres \
SCHEDULOR_DB_DSN='postgres://schedulor:schedulor@localhost:5432/schedulor?sslmode=disable' \
SCHEDULOR_EXECUTOR=bash \
SCHEDULOR_BASH_COMMANDS_FILE='./examples/commands/basic.json' \
SCHEDULOR_WITH_SCHEDULER=true \
go run ./cmd/schedulor
```

## 2. CLI + payload-команды (без файла)

Executor берёт команду из payload поля `command`.

```bash
SCHEDULOR_QUEUE_BACKEND=postgres \
SCHEDULOR_DB_DSN='postgres://schedulor:schedulor@localhost:5432/schedulor?sslmode=disable' \
SCHEDULOR_EXECUTOR=bash \
SCHEDULOR_EXECUTOR_TASK_NAMES='bash,ops' \
SCHEDULOR_EXECUTOR_COMMAND_FIELD=command \
SCHEDULOR_WITH_SCHEDULER=false \
go run ./cmd/schedulor
```

## 3. Запуск через код: команды из файла

```go
commands, err := schedulor.LoadBashTaskCommandsFromFile("./examples/commands/extended.json")
if err != nil {
    panic(err)
}
taskExec := schedulor.NewBashTaskExecutorWithCommands(commands)
```

Дальше передай `taskExec` в `schedulor.NewLqExecutor(...)`.

## 4. Запуск через код: in-memory конфиг команд

```go
taskExec := schedulor.NewBashTaskExecutorWithCommands(map[string]schedulor.BashTaskCommand{
    "health_ping": {Args: []string{"curl", "-fsS", "http://localhost:8080/health"}},
    "cleanup_tmp": {Command: "rm -rf /tmp/app-cache"},
})
```

## 5. Запуск через `fx`

```go
app, err := schedulor.NewFxApp(schedulor.FxAppOptions{
    Components: []schedulor.FxLifecycleComponent{
        lqExecutor,
        // lqScheduler, // опционально
    },
})
if err != nil {
    panic(err)
}
app.Run()
```

## 6. Запуск без PostgreSQL (smoke/runtime wiring)

Для проверки сборки runtime можно поднять CLI с `noop` backend:

```bash
SCHEDULOR_QUEUE_BACKEND=noop \
SCHEDULOR_EXECUTOR=bash \
SCHEDULOR_EXECUTOR_TASK_NAMES='bash' \
SCHEDULOR_EXECUTOR_COMMAND_FIELD=command \
SCHEDULOR_WITH_SCHEDULER=false \
go run ./cmd/schedulor
```

Этот режим не исполняет реальные задачи очереди, но полезен для smoke-проверки конфигурации и запуска процесса.
