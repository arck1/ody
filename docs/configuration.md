# Конфигурация

Новая архитектура конфигурируется явно через constructors и option structs. Она не читает
глобальное окружение, поэтому один процесс может поднять несколько worker с разными настройками.

## PostgreSQL Store

```go
db, err := sql.Open("pgx", dsn)
db.SetMaxOpenConns(30)
db.SetMaxIdleConns(10)
db.SetConnMaxLifetime(30 * time.Minute)

store, err := executionpostgres.New(db)
```

Приложение владеет `*sql.DB` и закрывает его самостоятельно. Worker, pipeline engine и monitoring
должны использовать один логический Store. Для нескольких процессов все они подключаются к одной
базе; claim использует блокировки PostgreSQL и безопасен для конкурирующих worker.

## Worker options

```go
worker.Options{
    ID:                "documents-eu-1",
    Concurrency:       16,
    PollInterval:      100 * time.Millisecond,
    LeaseDuration:     60 * time.Second,
    HeartbeatInterval: 20 * time.Second,
    MaxConsecutiveErrors: 10,
    ShutdownGracePeriod: 30 * time.Second,
}
```

| Поле | Назначение | Если не задано |
|---|---|---|
| `ID` | имя владельца lease для диагностики | случайный UUID |
| `Concurrency` | число параллельных polling loops | `1` |
| `PollInterval` | пауза, когда работы нет или claim завершился ошибкой | `100ms` |
| `LeaseDuration` | время владения claimed execution | `30s` |
| `HeartbeatInterval` | период продления lease | `LeaseDuration / 3` |
| `MaxConsecutiveErrors` | остановка после серии инфраструктурных сбоев; `0` означает бесконечный retry | `0` |
| `ShutdownGracePeriod` | ожидание handlers, которые не завершились после отмены context | `30s` |

`HeartbeatInterval` обязан быть меньше `LeaseDuration`. Lease выбирайте больше обычной сетевой
задержки и пауз runtime. Timeout задачи и lease — разные вещи: timeout ограничивает handler, lease
защищает владение записью.

## Redis Store

```go
client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
    DB:   0,
})

store, err := executionredis.New(client, executionredis.Options{
    Prefix: "billing:{execution}:",
})
```

Приложение владеет клиентом и закрывает его самостоятельно. Redis Store реализует полный контракт
`execution.Store`: историю и результаты, idempotency, pipeline, delayed queue, claim, lease,
heartbeat, retry, отмену и restart. Переходы одной записи атомарны через Redis transactions;
временные решения используют серверное время Redis.

Контракт разделён на `execution.Queue`, `execution.ExecutionRepository` и
`execution.PipelineRepository`; полный `execution.Store` объединяет их. Это позволяет прикладным
компонентам принимать только нужную часть API, при этом стандартные Store реализуют все три части.

Prefix по умолчанию — `ody:{execution}:`. Для Redis Cluster сохраняйте общий hash tag во
всех ключах одного Store. Разным окружениям и приложениям задавайте разные prefix.

Для production Redis используйте `maxmemory-policy noeviction`: частичное вытеснение execution,
индекса или event list нарушает целостность истории. Настройте AOF/RDB, replication и backup в
соответствии с допустимой потерей данных. Пароль/TLS задаются в конфигурации `go-redis`; библиотека
использует переданный `redis.UniversalClient`.

## Task options

```go
task.New[Input, Output]("documents.parse",
    task.WithVersion(2),
    task.WithMaxAttempts(5),
    task.WithTimeout(time.Minute),
    task.WithRetryPolicy(task.ExponentialBackoff(time.Second, time.Minute)),
)
```

Defaults: version `1`, max attempts `3`, без timeout, exponential backoff от `100ms` до `30s`.
Retry policy получает номер уже выполненной попытки.

## Статусы

Задача проходит через `pending`, `running`, `retry` и один из terminal statuses: `succeeded`,
`failed`, `cancelled`. Pipeline использует `pending`, `running`, `succeeded`, `failed`, `cancelled`.

`attempt` увеличивается при claim. `started_at` выставляется при первом claim, `finished_at` — при
terminal transition. Каждая смена состояния добавляет append-only event.

## Наблюдение за worker

Worker сообщает переходы выполнения через интерфейс `worker.Observer`; конкретный логгер ядру не
нужен. Для метрик передайте `monitoring/prometheus.Metrics`. Для логирования или tracing можно
реализовать собственный Observer и объединить несколько наблюдателей на уровне приложения.

Готовый zap adapter изолирован от worker core:

```go
observer, err := zapobserver.NewSugared(logger.Sugar())
app, err := ody.New(store, ody.Observe(observer))
```

`zapobserver.New` принимает небольшой интерфейс `Infow/Errorw`, поэтому совместим и с другими
структурированными логгерами без зависимости прикладного кода от zap.

## Переменные окружения CLI

Operational CLI:

- `ODY_DB_DSN` — PostgreSQL DSN;
- `ODY_STORE` — `postgres` (default) или `redis`;
- `ODY_REDIS_URL` — Redis URL, default `redis://127.0.0.1:6379/0`;
- `ODY_REDIS_PREFIX` — namespace ключей Redis;
- `ODY_ADMIN_ADDR` — адрес web server, default `127.0.0.1:8081`.
