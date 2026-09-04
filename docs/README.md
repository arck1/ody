# Документация Schedulor

Schedulor — Go-библиотека для фоновых задач, повторных попыток, сохранения результатов,
наблюдения за выполнениями и построения durable pipeline поверх Memory, PostgreSQL или Redis Store.

## С чего начать

1. [Быстрый старт](getting-started.md) — PostgreSQL, generic task и standalone worker.
2. [Конфигурация](configuration.md) — Store, worker, retry, lease, observer и окружение.
3. [Generic-задачи](tasks.md) — определения, handlers, модули, версии и постановка в очередь.
4. [Пайплайны](pipelines.md) — последовательности, fan-out/fan-in, результаты и восстановление.
5. [Запуск и эксплуатация](operations.md) — standalone, Fx, monitoring, Prometheus, CLI и web UI.

Основной API библиотеки — `task` + `execution.Store` + `worker` + `pipeline`. Он сохраняет активные
и завершённые выполнения, события, входы и результаты.

## Основные гарантии

- at-least-once доставка: handler обязан быть идемпотентным;
- lease token защищает завершение задачи от другого worker;
- вход и выход сохраняются как JSON;
- результат узла сохраняется до запуска зависимых узлов;
- retry и состояние pipeline переживают рестарт процесса;
- `(task_name, idempotency_key)` защищает standalone enqueue от дублей;
- `(pipeline_run_id, node_key)` защищает узлы pipeline от повторного создания.
