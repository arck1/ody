# Ody Agent Guide

This file is the source of truth for coding agents working in this repository. `README.md` and
`docs/` are user-facing and must stay consistent with public behavior.

## Project purpose

Ody is a Go library for durable, typed background jobs and persistent DAG pipelines. It
supports standalone construction and optional Uber Fx lifecycle integration. PostgreSQL and Redis
are production Stores; the memory Store supports tests and local scenarios.

The module path is `github.com/arck1/ody`. The required Go version is declared in `go.mod`.

## Architecture

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
    Memory / PostgreSQL / Redis    persistent DAG
              │
              ▼
 monitoring.Service ──► CLI / JSON API / Web UI / store metrics
```

`execution.Store` is the system of record for active and terminal executions, inputs, results,
attempts, leases, events, and pipeline runs. Infrastructure boundaries use JSON; task handlers and
pipeline mappers provide typed conversion.

The full Store embeds three capability interfaces: `execution.Queue`,
`execution.ExecutionRepository`, and `execution.PipelineRepository`. Prefer the narrowest capability
when a new component does not need the full Store.

## Package map

- `task`: generic definitions, handlers, modules, registry, retry policies, permanent failures,
  delayed retries, and enqueue.
- `execution`: execution/run/event models and the persistence Store contract.
- `execution/postgres`: PostgreSQL Store and embedded idempotent migration.
- `execution/redis`: Redis Store; sorted-set delivery queue, optimistic transactions, leases,
  history, idempotency, and pipeline persistence.
- `pipeline`: typed DAG definition (`Start`, `Then`, `Join2`), registry, durable engine,
  reconciliation, cancellation, inspection, and output decoding.
- `schedule`: cron definitions for typed tasks and pipelines, timezone, misfire recovery, overlap
  policy, and durable tick idempotency.
- `worker`: standalone concurrent worker with polling, timeout, lease heartbeat, retry, result
  persistence, observer callbacks, and pipeline advancement.
- root `ody.App`: common standalone/Fx composition facade over Store, worker, and pipeline.
- `worker/fx`: low-level Fx lifecycle adapter for custom worker assembly.
- `monitoring`: transport-neutral read/control interfaces and service.
- `monitoring/prometheus`: worker observer metrics and persisted-state collector.
- `monitoring/httpui`: embedded operator dashboard and JSON API.
- `cmd/ody-admin`: PostgreSQL-backed operational CLI and web server.
- `functional`: public-API behavior suites; integration-tagged suites use real PostgreSQL and Redis.

The root package is the preferred composition API; low-level packages remain available for custom
assembly.

## Execution invariants

Statuses follow `pending -> running -> succeeded|retry|failed|cancelled`, with `retry` becoming
claimable again. Preserve these rules:

- Task identity is `(name, version)`.
- `attempt` increments only on claim.
- A running execution is owned by `(lease_owner, lease_token, lease_until)`; stale completion must
  return `execution.ErrLeaseLost`.
- Success persists output before advancing a pipeline.
- Retry clears the lease and schedules `available_at`, or becomes terminal after the final attempt.
- Decode/encode failures and unknown task versions are permanent.
- Handler panics are recovered and persisted; they never terminate the worker.
- `ReapExpired` terminally fails an expired final attempt and reports affected pipeline IDs.
- Standalone idempotency is unique by `(task_name, idempotency_key)`.
- Pipeline nodes are unique by `(pipeline_run_id, node_key)`.
- Restart retains identity and events, appends `restarted`, resets mutable execution state, and
  reopens an attached pipeline.

Memory and PostgreSQL stores must have identical observable semantics. Update both implementations
and their tests whenever `execution.Store` changes.

## Pipeline rules

- Pipelines are versioned and registered separately from tasks.
- Nodes are declared in dependency order; forward and cross-pipeline dependencies are invalid.
- A node is scheduled only after all predecessors durably succeed.
- Mappers read stored predecessor outputs, never process-local results.
- A failed or cancelled node fails the run and prevents unscheduled descendants from starting.
- The same registry must be available after restart so `Reconcile` can resume DAGs.
- `pipeline.Engine.Advance` is idempotent; preserve this property.
- Pipeline run transitions use `revision` compare-and-set; stale coordinators retry from persisted
  state and must not overwrite a newer terminal decision.

## Monitoring and operations

`monitoring.API` composes `TaskReader`, `PipelineReader`, and `Controller`. Keep transports thin:
business rules belong in the service/store, not HTTP handlers or CLI commands.

The admin server defaults to `127.0.0.1:8081` and has no authentication. Production exposure
requires an authenticated TLS reverse proxy. Treat task input/output as sensitive and untrusted.
Prometheus labels must stay low-cardinality: names and statuses are acceptable; execution IDs,
errors, inputs, and outputs are not.

## Construction and lifecycle

- Prefer constructors and explicit interfaces; reject nil mandatory dependencies.
- Base components run without Fx. Fx lifecycle code belongs only in adapters.
- `worker.Run` blocks until context cancellation and drains polling goroutines. Handlers must honor
  their context because Go cannot forcibly stop one that ignores cancellation.
- Long-lived components must honor cancellation promptly and close owned resources.
- Constructors receiving database or Redis clients do not own them; the application closes them.
- Observability depends on `worker.Observer`, not a concrete logger.
- `worker/zapobserver` is the optional zap adapter; logger compatibility is expressed through its
  small `Infow/Errorw` interface.

## Database changes

- The PostgreSQL schema is `execution/postgres/schema.sql` and is embedded by the Store.
- Preserve constraints and indexes enforcing idempotency and pipeline-node uniqueness.
- Migrations must be safe to apply repeatedly and need integration coverage.
- Dynamic SQL may select only known fragments; all values remain parameterized.
- Redis mutations use WATCH transactions, server time for queue/lease decisions, and one common
  Cluster hash tag in the key prefix.

## Tests and quality gates

```bash
make fmt
make lint
make check
make test-functional-integration # real PostgreSQL and Redis via Testcontainers
make test-integration
```

Integration files use `//go:build integration` and skip when Docker is unavailable. Use
`stretchr/testify`; keep scenario groups in separate suite files. Test public APIs for functional
behavior and cover success, retry, permanent failure, cancellation, stale lease, idempotency,
restart, and pipeline fan-out/fan-in as relevant.

For integration-tag lint:

```bash
GOCACHE="$PWD/bin/go-build-cache" \
GOLANGCI_LINT_CACHE="$PWD/bin/golangci-cache" \
./bin/golangci-lint run --build-tags integration ./functional
```

## Change discipline

- Preserve unrelated user changes.
- Use `apply_patch` for hand edits.
- Keep public docs aligned with constructors and CLI flags.
- Add dependencies deliberately and avoid package cycles from adapters into core packages.
- Verify formatting, lint, focused tests, the full suite, and integration compilation in proportion
  to the change.
- Commit each completed logical change to git; never include unrelated files.

## Known limitations

- Redis list operations currently read the selected sorted-set index and filter records client-side.
- The embedded web UI is an operator tool, not a multi-tenant control plane.
- Pipeline construction provides single-predecessor `Then` and two-input `Join2`.
