# Пайплайны

Pipeline — версионированный durable DAG. Каждый узел является обычной generic-задачей, а его output
сохраняется в Store до построения input следующего узла.

## Последовательность

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
```

`Start` строит input из начального input pipeline. `Then` получает типизированный output одного
предшественника. Node key (`fetch`, `parse`) стабилен внутри версии pipeline и уникален.

## Fan-out и fan-in

```go
content := pipeline.Start(flow, "fetch-content", FetchContent, mapContentInput)
metadata := pipeline.Start(flow, "fetch-metadata", FetchMetadata, mapMetadataInput)

indexed := pipeline.Join2(flow, content, metadata, "index", Index,
    func(document Document, meta Metadata) IndexInput {
        return IndexInput{Document: document, Metadata: meta}
    },
)
```

Root nodes могут выполняться параллельно. `Join2` создаётся только после успеха обоих входов.
Компилятор проверяет типы output и mapper input.

## Регистрация и запуск

```go
app, err := schedulor.New(store,
    schedulor.Tasks(taskModule),
    schedulor.Pipelines(flow),
    schedulor.Observe(observer),
    schedulor.WithWorker(workerOptions),
)

run, err := pipeline.Run(ctx, app.PipelineEngine(), flow, ImportInput{URL: url})
err = app.Run(ctx)
```

Все task definitions, используемые узлами, должны быть в task registry worker. Все активные версии
pipeline должны быть в pipeline registry каждого процесса, способного выполнять `Advance` или
`Reconcile`.

## Состояние и результат

```go
snapshot, err := app.PipelineEngine().Inspect(ctx, run.ID)
result, err := pipeline.Output(ctx, store, run.ID, indexed)
```

`Output` возвращает ошибку, пока выбранный узел не завершился успешно.

## Ошибки, отмена и восстановление

- Failed/cancelled node переводит весь run в `failed` и не создаёт незапущенных descendants.
- `app.PipelineEngine().Cancel(ctx, runID, reason)` отменяет незавершённые nodes и сам run.
- `app.RestartExecution(ctx, executionID)` безопасно сбрасывает узел и существующих потомков.
- Worker вызывает `Advance` после terminal transition node.
- `engine.Reconcile(ctx)` продолжает `pending`/`running` pipelines после process crash между
  сохранением результата и созданием следующего узла.

Запускайте `Reconcile` при старте приложения или полагайтесь на worker: при idle polling он также
вызывает reconciliation, если переданный advancer поддерживает этот интерфейс.

## Версионирование

Меняйте версию pipeline при несовместимой структуре DAG, node keys или mapper semantics. Старую
definition нельзя удалять, пока существуют незавершённые runs этой версии. Task version и pipeline
version независимы.

Restart pipeline node через monitoring API повторно открывает run. Уже успешные downstream nodes
автоматически не сбрасываются; если бизнес-процесс требует полного replay, создавайте новый run или
реализуйте отдельную стратегию reset для нужного подграфа.
