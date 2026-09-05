# schedulor

Schedulor — модульная Go-библиотека для типизированных фоновых задач и durable pipeline.
Выполнения, входы, результаты, попытки, lease и история переходов сохраняются через
`execution.Store`; базовые реализации — in-memory, PostgreSQL и Redis.

## Возможности

- generic-задачи `task.Definition[Input, Output]` и типизированные handlers;
- конкурентный worker с timeout, retry, heartbeat и at-least-once выполнением;
- persistent pipeline с `Start`, `Then`, fan-out и `Join2`;
- standalone-запуск и опциональная интеграция с Uber Fx;
- просмотр, отмена и перезапуск выполнений через Go API, CLI и web UI;
- Prometheus-метрики и функциональные тесты с настоящими PostgreSQL и Redis.

## Быстрый пример

```go
sendEmail := task.New[EmailInput, EmailResult](
	"email.send",
	task.WithMaxAttempts(5),
	task.WithTimeout(30*time.Second),
)

module, err := task.NewModule("email",
	task.Handle(sendEmail, func(ctx context.Context, message task.Message[EmailInput]) (EmailResult, error) {
		return mailer.Send(ctx, message.Input)
	}),
)
store := execution.NewMemoryStore()
app, err := schedulor.New(store,
	schedulor.Tasks(module),
	schedulor.WithWorker(worker.Options{Concurrency: 4}),
)

created, err := sendEmail.Enqueue(ctx, app, EmailInput{To: "user@example.com"})
err = app.Run(ctx)
```

В production используйте `execution/postgres.Store` или `execution/redis.Store` и передавайте один
Store в worker, pipeline engine и monitoring service. PostgreSQL требует вызова `Migrate`; Redis
не требует отдельной схемы. Handler должен быть идемпотентным: модель доставки at-least-once.

## Документация

- [Быстрый старт](docs/getting-started.md)
- [Конфигурация](docs/configuration.md)
- [Generic-задачи](docs/tasks.md)
- [Пайплайны](docs/pipelines.md)
- [Запуск, Fx, CLI, web UI и метрики](docs/operations.md)
- [Архитектура для агентов](AGENTS.md)

## Проверки

```bash
make check
make test-functional-integration # нужен Docker; поднимает PostgreSQL и Redis через Testcontainers
```

Версия `golangci-lint` зафиксирована и устанавливается локально командой `make install-lint`.

## Operational UI

```bash
export SCHEDULOR_DB_DSN='postgres://schedulor:schedulor@localhost:5432/schedulor?sslmode=disable'
go run ./cmd/schedulor-admin --addr 127.0.0.1:8081 serve
```

Для Redis: `SCHEDULOR_STORE=redis SCHEDULOR_REDIS_URL=redis://127.0.0.1:6379/0`.

Dashboard будет доступен на `http://127.0.0.1:8081/`, метрики — на `/metrics`, JSON API — под
`/api/tasks` и `/api/pipelines`. Перед публикацией наружу добавьте authentication и TLS на reverse
proxy.
