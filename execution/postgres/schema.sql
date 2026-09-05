CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    pipeline_version INT NOT NULL,
    input JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','failed','cancelled')),
    error TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ
);

ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS pipeline_runs_idempotency_idx
    ON pipeline_runs(pipeline_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS task_executions (
    id UUID PRIMARY KEY,
    task_name TEXT NOT NULL,
    task_version INT NOT NULL,
    input JSONB NOT NULL,
    output JSONB,
    status TEXT NOT NULL CHECK (status IN ('pending','blocked','running','retry','succeeded','failed','cancelled')),
    attempt INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL CHECK (max_attempts > 0),
    available_at TIMESTAMPTZ NOT NULL,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT,
    pipeline_run_id UUID REFERENCES pipeline_runs(id),
    node_key TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS task_executions_idempotency_idx
    ON task_executions(task_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS task_executions_pipeline_node_idx
    ON task_executions(pipeline_run_id, node_key)
    WHERE pipeline_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS task_executions_claim_idx
    ON task_executions(status, available_at, task_name);

CREATE INDEX IF NOT EXISTS task_executions_run_idx
    ON task_executions(pipeline_run_id, created_at);

CREATE TABLE IF NOT EXISTS execution_events (
    id UUID PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES task_executions(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    attempt INT NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS execution_events_execution_idx
    ON execution_events(execution_id, created_at);

-- Existing installations may still have the pre-pipeline-restart status constraint.
ALTER TABLE task_executions DROP CONSTRAINT IF EXISTS task_executions_status_check;
ALTER TABLE task_executions ADD CONSTRAINT task_executions_status_check
    CHECK (status IN ('pending','blocked','running','retry','succeeded','failed','cancelled'));
