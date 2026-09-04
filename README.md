# schedulor

Schedulor — модульная Go-библиотека для типизированных фоновых задач и durable pipeline.
Выполнения, входы, результаты, попытки, lease и история переходов сохраняются через
`execution.Store`; базовые реализации — in-memory и PostgreSQL.

## Возможности

- generic-задачи `task.Definition[Input, Output]` и типизированные handlers;
- конкурентный worker с timeout, retry, heartbeat и at-least-once выполнением;
- persistent pipeline с `Start`, `Then`, fan-out и `Join2`;
- standalone-запуск и опциональная интеграция с Uber Fx;
- просмотр, отмена и перезапуск выполнений через Go API, CLI и web UI;
- Prometheus-метрики и функциональные тесты с настоящим PostgreSQL.

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
registry, err := task.NewRegistry(module)

store := execution.NewMemoryStore()
runner, err := worker.New(store, registry, nil, nil, worker.Options{Concurrency: 4})

created, err := sendEmail.Enqueue(ctx, store, EmailInput{To: "user@example.com"})
err = runner.Run(ctx)
```

В production используйте `execution/postgres.Store`, вызовите `Migrate` при развёртывании и
передавайте один Store в worker, pipeline engine и monitoring service. Handler должен быть
идемпотентным: модель доставки at-least-once.

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
make test-functional-integration # нужен Docker; поднимает PostgreSQL через Testcontainers
```

Версия `golangci-lint` зафиксирована и устанавливается локально командой `make install-lint`.

## Operational UI

```bash
export SCHEDULOR_DB_DSN='postgres://schedulor:schedulor@localhost:5432/schedulor?sslmode=disable'
go run ./cmd/schedulor-admin --addr 127.0.0.1:8081 serve
```

Dashboard будет доступен на `http://127.0.0.1:8081/`, метрики — на `/metrics`, JSON API — под
`/api/tasks` и `/api/pipelines`. Перед публикацией наружу добавьте authentication и TLS на reverse
proxy.
