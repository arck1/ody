# Cron-расписания

Scheduler не имеет отдельной очереди: каждый cron tick создаёт обычную durable execution или
pipeline run. Поэтому задачи обрабатываются теми же worker, видны в monitoring API и сохраняют
обычные retry/lease semantics.

## Задача по расписанию

```go
dailyReport, err := schedule.Task(
    "daily-report",
    "0 7 * * *",
    BuildReport,
    func(scheduledAt time.Time) ReportInput {
        return ReportInput{Day: scheduledAt.Format("2006-01-02")}
    },
    schedule.WithLocation(moscow),
    schedule.WithOverlap(schedule.OverlapSkip),
    schedule.WithMisfire(schedule.MisfireLatest, 24*time.Hour, 1),
)

app, err := schedulor.New(store,
    schedulor.Tasks(reportModule),
    schedulor.Schedules(dailyReport),
)
```

Каждый tick получает idempotency key вида `schedule:<name>:<UTC instant>`. Несколько экземпляров
приложения могут одновременно наблюдать один cron tick: Store создаст только одну execution.

## Pipeline по расписанию

```go
nightlyImport, err := schedule.Pipeline(
    "nightly-import",
    "0 2 * * *",
    importPipeline,
    func(at time.Time) ImportInput { return ImportInput{Date: at} },
)

app, err := schedulor.New(store,
    schedulor.Tasks(importTasks),
    schedulor.Pipelines(importPipeline),
    schedulor.Schedules(nightlyImport),
)
```

Pipeline runs также имеют durable idempotency key, поэтому повторная доставка одного tick безопасна.

## Политики

- `MisfireSkip` — после запуска не восстанавливать пропущенные ticks;
- `MisfireLatest` — выполнить последний tick в пределах lookback;
- `MisfireCatchUp` — выполнить пропущенные ticks до `maxCatchUp`;
- `OverlapAllow` — разрешить новую execution, пока предыдущая активна;
- `OverlapSkip` — не создавать новый запуск при `pending`, `retry` или `running` задаче/pipeline.

Timezone передаётся через `WithLocation`. В Store и idempotency keys момент сохраняется в UTC.
