# Generic-задачи

## Definition

`task.Definition[Input, Output]` — типизированный и версионированный контракт. Имя должно быть
стабильным и глобально уникальным в приложении, например `billing.invoice.generate`.

```go
GenerateInvoice := task.New[InvoiceInput, Invoice](
    "billing.invoice.generate",
    task.WithVersion(1),
    task.WithMaxAttempts(4),
)
```

Не переиспользуйте старую версию имени для несовместимого JSON. Зарегистрируйте новый handler с
`WithVersion(2)` и держите v1 доступной, пока в Store могут оставаться старые executions.

## Handler и Message

```go
binding := task.Handle(GenerateInvoice,
    func(ctx context.Context, message task.Message[InvoiceInput]) (Invoice, error) {
        log.Printf("execution=%s attempt=%d/%d",
            message.ExecutionID, message.Attempt, message.MaxAttempts)
        return invoices.Generate(ctx, message.Input)
    },
)
```

Schedulor декодирует сохранённый JSON input и кодирует output. Ошибка декодирования/кодирования
считается permanent. Panic handler перехватывается и превращается в ошибку выполнения.

Handler может выполниться повторно после потери lease или сбоя между внешним side effect и записью
результата. Используйте execution/idempotency key при вызове внешних систем.

## Ошибки и retry

Обычная ошибка использует retry policy definition:

```go
return Invoice{}, fmt.Errorf("generate invoice: %w", err)
```

Задержка конкретной попытки:

```go
return Invoice{}, task.RetryAfter(err, 30*time.Second)
```

Ошибка без повторов:

```go
return Invoice{}, task.Permanent(ErrInvalidCustomer)
```

После последней попытки execution становится `failed`. Текст ошибки сохраняется в `last_error` и
event history; не помещайте в него секреты.

## Modules и Registry

Группируйте связанные handlers в модуль:

```go
billingModule, err := task.NewModule("billing",
    task.Handle(GenerateInvoice, generateInvoice),
    task.Handle(SendInvoice, sendInvoice),
)

documentsModule, err := task.NewModule("documents",
    task.Handle(ParseDocument, parseDocument),
)

registry, err := task.NewRegistry(billingModule, documentsModule)
```

Registry запрещает дубликаты `(name, version)`. Worker claims только имена, присутствующие в его
registry, поэтому разные worker pools могут обслуживать разные модули из одной базы.

## Standalone enqueue

```go
created, err := GenerateInvoice.Enqueue(ctx, store, InvoiceInput{OrderID: orderID},
    task.WithIdempotencyKey("invoice:"+orderID),
    task.WithAvailableAt(time.Now().UTC().Add(time.Minute)),
)
```

При одинаковых task name и непустом idempotency key возвращается существующее execution. Если
нужно сознательно создать новое выполнение, используйте новый ключ или не передавайте его.

## Наблюдение и управление

```go
service, err := monitoring.New(store)
details, err := service.Task(ctx, created.ID) // execution + events
restarted, err := service.RestartTask(ctx, created.ID)
err = service.CancelTask(ctx, created.ID, "cancelled by user")
```

Restart разрешён только для terminal execution. Он сохраняет ID и историю, очищает старый result,
error и lease, сбрасывает attempt и снова переводит задачу в `pending`.

