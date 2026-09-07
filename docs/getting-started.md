# Быстрый старт

Ниже минимальное приложение на новой архитектуре. Оно создаёт PostgreSQL Store, регистрирует
типизированную задачу, ставит выполнение и запускает worker до отмены контекста.

## 0. Подключите модуль

Текущий module path проекта — `ody`. Для локального приложения рядом с checkout:

```bash
go mod edit -require=ody@v0.0.0
go mod edit -replace=ody=../ody
go mod tidy
```

После публикации библиотеки замените module path и imports на адрес репозитория с выбранной
semantic version. В примерах используются imports `ody/task`, `ody/execution`,
`ody/execution/postgres`, `ody/pipeline` и `ody/worker`.

Нужны версия Go из `go.mod` и PostgreSQL либо Redis для durable production Store.
`execution.MemoryStore` подходит для тестов и локальных однопроцессных сценариев.

## 1. Подготовьте PostgreSQL

Для локальной разработки зависимости уже описаны в `docker-compose.yml`:

```bash
docker compose up -d postgres
```

## 2. Создайте Store и примените схему

```go
db, err := sql.Open("pgx", postgresDSN)
if err != nil {
    return err
}
defer db.Close()

store, err := executionpostgres.New(db)
if err != nil {
    return err
}
if err = store.Migrate(ctx); err != nil {
    return err
}
```

`Migrate` идемпотентно создаёт `task_executions`, `execution_events`, `pipeline_runs`, индексы и
ограничения. В production миграцию можно выполнять отдельным deploy step.

Эквивалентный Redis Store не требует миграции:

```go
client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
defer client.Close()

store, err := executionredis.New(client, executionredis.Options{
    Prefix: "myapp:{execution}:",
})
```

Redis хранит executions, события и pipeline, а sorted sets используются как очередь delayed-задач
и lease. Для Redis Cluster все ключи Store должны оставаться в одном hash slot, поэтому prefix
должен содержать общий hash tag, например `{execution}`.

## 3. Опишите задачу и handler

```go
type EmailInput struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

type EmailResult struct {
    MessageID string `json:"message_id"`
}

SendEmail := task.New[EmailInput, EmailResult](
    "email.send",
    task.WithVersion(1),
    task.WithMaxAttempts(5),
    task.WithTimeout(20*time.Second),
)

emailModule, err := task.NewModule("email",
    task.Handle(SendEmail, func(ctx context.Context, message task.Message[EmailInput]) (EmailResult, error) {
        id, err := mailer.Send(ctx, message.Input.To, message.Input.Subject)
        if err != nil {
            return EmailResult{}, err
        }
        return EmailResult{MessageID: id}, nil
    }),
)
if err != nil {
    return err
}

app, err := ody.New(store,
    ody.Tasks(emailModule),
    ody.WithWorker(worker.Options{
        ID:                "email-worker-1",
        Concurrency:       8,
        PollInterval:      100 * time.Millisecond,
        LeaseDuration:     30 * time.Second,
        HeartbeatInterval: 10 * time.Second,
    }),
)
if err != nil {
    return err
}
```

## 4. Поставьте задачу

```go
created, err := SendEmail.Enqueue(ctx, app, EmailInput{
    To:      "user@example.com",
    Subject: "Your report is ready",
}, task.WithIdempotencyKey("report-email:"+reportID))
if err != nil {
    return err
}

log.Printf("execution id: %s", created.ID)
```

Можно задать отложенный запуск:

```go
created, err := SendEmail.Enqueue(ctx, app, input,
    task.WithAvailableAt(time.Now().UTC().Add(15*time.Minute)),
)
```

## 5. Запустите worker

```go
if err = app.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    return err
}
```

`Run` блокируется. Обычно его запускают из `errgroup`, service runner или через Fx adapter.
Handler должен завершаться при отмене `ctx`: Go не может принудительно остановить зависший handler.

## 6. Получите состояние и результат

```go
executionItem, err := store.GetExecution(ctx, created.ID)
events, err := store.Events(ctx, created.ID)

var result EmailResult
if executionItem.Status == execution.StatusSucceeded {
    err = json.Unmarshal(executionItem.Output, &result)
}
```

Для polling в прикладном API проверяйте terminal statuses: `succeeded`, `failed`, `cancelled`.
