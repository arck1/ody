# Schedulor Agent Guide

This file is the source of truth for coding agents working in this repository. Read it before
changing code. `README.md` is the user-facing guide; keep both documents consistent when public
behavior changes.

## Project purpose

Schedulor is a Go library for durable background jobs, cron-triggered work, typed task execution,
and persistent DAG pipelines. It supports standalone construction and optional Uber Fx lifecycle
integration. PostgreSQL is the durable store for the current typed architecture. Redis and
PostgreSQL are queue backends for the earlier `LqExecutor` architecture.

The module path is `schedulor`. The required Go version is declared in `go.mod`.

## Architectural boundaries

There are two execution paths. Do not accidentally mix their contracts.

### Current typed architecture

```text
task.Definition[I,O] + typed handler
              │
              ▼
        task.Registry
              │
              ▼
         worker.Worker ─────► worker.Observer / Prometheus
              │
              ▼
       execution.Store ◄──── pipeline.Engine
              │                    │
       Memory / PostgreSQL     persistent DAG
              │
              ▼
 monitoring.Service ──► CLI / JSON API / Web UI / store metrics
```

Use this architecture for new features. `execution.Store` is the durable system of record for
active and terminal executions, results, attempts, leases, events, and pipeline runs.

### Legacy queue architecture

```text
LqScheduler ─► queue.TasksQueue ─► queue.Backend ─► LqExecutor ─► TaskExecutor
                                      │
                               PostgreSQL / Redis
```

This path remains supported for existing users and the cron scheduler. A successful `Ack` removes
the queue record, so it cannot provide completed-task history. Do not use it as the data source for
the monitoring UI. `queue.KafkaQueue` is still a placeholder returning `queue.ErrNotImplemented`.

Breaking public contracts is allowed only when the requested change needs it. Prefer the typed
architecture over expanding legacy abstractions.

## Package map

- `task`: generic task definitions, typed handlers, modules, registry, retry policies, permanent
  failures, delayed retries, and standalone enqueue.
- `execution`: execution/run/event models and the persistence `Store` contract.
- `execution/postgres`: PostgreSQL implementation and embedded schema migration.
- `pipeline`: typed DAG definition (`Start`, `Then`, `Join2`), registry, durable engine,
  reconciliation, cancellation, inspection, and output decoding.
- `worker`: standalone concurrent worker with polling, handler timeout, lease heartbeat, retry,
  result persistence, observer callbacks, and pipeline advancement.
- `worker/fx`: optional Fx lifecycle adapter. The base worker must stay independent of Fx.
- `monitoring`: transport-neutral read/control interfaces and service.
- `monitoring/prometheus`: worker observer metrics and persisted-state collector.
- `monitoring/httpui`: embedded operator dashboard and JSON API.
- `queue`: legacy `Backend` interface plus PostgreSQL, Redis, and placeholder Kafka backends.
- `executor`: legacy code and command execution strategies.
- `elector`: static and PostgreSQL leader election for cron scheduling.
- `cmd/schedulor`: legacy worker/scheduler CLI.
- `cmd/schedulor-admin`: PostgreSQL-backed operational CLI and web server.
- `functional`: public-API behavior suites; integration-tagged suites use real containers.

Root package files mostly expose the legacy scheduler/executor APIs, logging abstraction, settings,
and Fx application assembly.

## Typed execution lifecycle

Execution statuses are:

```text
pending ─► running ─► succeeded
                   ├► retry ─► running
                   ├► failed
                   └► cancelled
```

Important invariants:

- The identity of a task definition is `(name, version)`.
- Inputs and outputs cross infrastructure boundaries as JSON. Typed conversion happens in the task
  binding and pipeline mapper.
- `attempt` increments only when a worker claims an execution.
- A running execution is owned by `(lease_owner, lease_token, lease_until)`. Mutating completion
  methods must reject stale tokens with `execution.ErrLeaseLost`.
- Worker success must persist output before advancing a pipeline.
- Retry clears the lease and either schedules `available_at` or becomes terminal when attempts are
  exhausted.
- Decode/encode failures and unknown task versions are permanent failures.
- Handler panics are recovered and persisted as failures; do not allow them to terminate workers.
- `ReapExpired` terminally fails an expired final attempt and reports affected pipeline IDs.
- Standalone idempotency is unique by `(task_name, idempotency_key)`.
- Pipeline nodes are unique by `(pipeline_run_id, node_key)`.
- Restart is allowed only for `succeeded`, `failed`, or `cancelled` executions. It retains identity
  and event history, appends `restarted`, clears result/error/lease/timestamps, resets attempts, and
  reopens an attached pipeline as `running`.

The PostgreSQL and memory stores must implement identical observable semantics. When changing the
`execution.Store` interface, update both implementations and add contract-level tests.

## Pipeline rules

- Pipelines are versioned and registered separately from tasks.
- Nodes must be declared in dependency order; forward or cross-pipeline dependencies are invalid.
- A node is scheduled only after all predecessors have durably succeeded.
- Mapper inputs come from stored predecessor outputs, not process memory.
- Any failed/cancelled node fails the run and prevents unscheduled descendants from starting.
- The same pipeline registry must be available after process restart so `Reconcile` can resume DAGs.
- `pipeline.Engine.Advance` is intentionally idempotent; preserve that property.

## Queue backend rules

The legacy `queue.Backend` consists of `TasksQueue` and `WorkerQueue`.

- Enqueue supports optional idempotency and delayed availability.
- Claim must be atomic, ordered, bounded, and safe under multiple workers.
- Ack/Nack/DLQ mutations must be lease-token guarded.
- Heartbeat must never revive an already expired or replaced lease.
- PostgreSQL uses row locks with `FOR UPDATE SKIP LOCKED`.
- Redis state transitions use Lua scripts. Redis server time is used for lease decisions.
- Redis Cluster keys must share a hash slot; preserve the common hash tag in prefixes such as
  `schedulor:{queue}:`.
- Redis `Ack` deletes active data and its idempotency entry. DLQ records are retained.

## Monitoring and operator API

`monitoring.API` composes `TaskReader`, `PipelineReader`, and `Controller`. Keep transports thin:
business rules belong in the service/store, not HTTP handlers or CLI commands.

HTTP endpoints:

- `GET /api/tasks` with `status`, `task_name`, and `limit` filters.
- `GET /api/tasks/{id}` for execution content and events.
- `POST /api/tasks/{id}/restart`.
- `POST /api/tasks/{id}/cancel?reason=...`.
- `GET /api/pipelines` with repeated `status` filters.
- `GET /api/pipelines/{id}` for a run and its node executions.
- `GET /metrics` and `GET /` for Prometheus and the embedded dashboard.

The admin server defaults to `127.0.0.1:8081` and has no built-in authentication. Production
exposure requires an authenticated TLS reverse proxy. Treat task input/output as potentially
sensitive and untrusted; preserve JSON escaping and never inject values into HTML directly.

Prometheus labels must remain low-cardinality. Task and pipeline names are acceptable labels;
execution IDs, idempotency keys, errors, inputs, and outputs are not.

## Construction and lifecycle

- Prefer constructors and explicit interfaces; reject nil mandatory dependencies.
- Base components must run without Fx. Put Fx-specific lifecycle code only in adapters.
- `worker.Run` blocks until context cancellation and drains its polling goroutines. Task handlers
  must honor their context; Go cannot forcibly stop a handler that ignores cancellation.
- Long-lived components must honor cancellation promptly and close owned resources.
- Constructors receiving a database or Redis client do not own it unless their documentation says
  otherwise. The application closes clients.
- Core packages depend on `schedulor.Logger`, not directly on zap. `NewZapLogger` is the default
  adapter.

## Database changes

- Typed architecture schema: `execution/postgres/schema.sql`, embedded by the PostgreSQL store.
- Legacy scheduler/queue schema: root `schema.sql`.
- Preserve constraints and indexes that enforce idempotency and pipeline-node uniqueness.
- Schema changes must be safe to apply repeatedly. Add integration coverage for new SQL behavior.
- Never build SQL from untrusted values. Dynamic query fragments may only select known clauses;
  values stay parameterized.

## Tests and quality gates

Use the repository commands instead of ad-hoc variants when possible:

```bash
make fmt                         # format with pinned golangci-lint
make lint                        # lint normal build
make check                       # formatting + lint + unit/functional tests
make test-functional-integration # real PostgreSQL and Redis via Testcontainers
make test-integration            # all integration-tagged tests
```

Integration files use `//go:build integration`. Docker must be running for infrastructure suites;
they skip when no provider is available. GitHub Actions has a separate job that runs the real
PostgreSQL/Redis suites.

Test expectations:

- Use `stretchr/testify`; scenario groups should be separate suite files.
- Test through public APIs for functional scenarios.
- Use `MemoryStore` for deterministic domain behavior and Testcontainers for storage/protocol
  contracts.
- Cover success, retry, permanent failure, cancellation, stale lease, idempotency, restart, and
  pipeline fan-out/fan-in when changing those paths.
- Run integration-tag lint for tagged-only code:

```bash
GOCACHE="$PWD/bin/go-build-cache" \
GOLANGCI_LINT_CACHE="$PWD/bin/golangci-cache" \
./bin/golangci-lint run --build-tags integration ./functional
```

## Change discipline

- Preserve unrelated user changes in a dirty worktree.
- Use `apply_patch` for hand edits.
- Keep public docs and examples aligned with constructor signatures and CLI flags.
- Add dependencies deliberately and keep direct imports in the direct `require` block.
- Do not introduce package cycles from adapters back into core packages.
- Verify formatting, lint, focused tests, the full test suite, and integration compilation in
  proportion to the change.
- Commit each completed logical change to git. Do not include unrelated files in the commit.

## Known limitations

- Redis implements the legacy queue contract, not `execution.Store`; completed Redis tasks are not
  available in the monitoring history after Ack.
- Kafka/Redpanda queue execution is not implemented yet.
- The operational CLI and web UI currently read the PostgreSQL execution store only.
- The embedded web UI is an operator tool, not a multi-tenant control plane.
- Pipeline construction currently provides single-predecessor `Then` and two-input `Join2`; add
  further combinators without weakening compile-time output typing.
