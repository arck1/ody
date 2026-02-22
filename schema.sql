CREATE TABLE IF NOT EXISTS lq_tasks
(
    task_id         BIGSERIAL PRIMARY KEY,
    task_name       VARCHAR     NOT NULL,
    payload         JSONB,
    available_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    reserved_until  TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    lease_token     UUID,
    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 25,
    last_error      TEXT,
    idempotency_key VARCHAR,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lq_tasks_dlq
(
    LIKE lq_tasks INCLUDING ALL
);

CREATE INDEX IF NOT EXISTS lq_tasks_ready_idx
    ON lq_tasks (task_name, available_at, reserved_until)
    WHERE attempts <= max_attempts;

CREATE INDEX IF NOT EXISTS queue_created_at_idx ON lq_tasks (created_at);

CREATE UNIQUE INDEX IF NOT EXISTS queue_idem_uniq
    ON lq_tasks (task_name, idempotency_key) WHERE idempotency_key IS NOT NULL;


CREATE TABLE IF NOT EXISTS lq_schedules
(
    id          UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    task_name   varchar(256) NOT NULL,
    cron        varchar(256) NOT NULL,
    payload     JSONB,
    is_active   boolean               default false,
    next_run    timestamptz,
    last_run    timestamptz,
    task_id     bigint,
    updated     timestamptz  NOT NULL default now(),
    description text
);

CREATE OR REPLACE FUNCTION set_lq_schedule_updated_if_changed()
    RETURNS trigger AS
$$
BEGIN
    IF (OLD.task_name IS DISTINCT FROM NEW.task_name) OR
       (OLD.cron IS DISTINCT FROM NEW.cron) OR
       (OLD.is_active IS DISTINCT FROM NEW.is_active) THEN
        NEW.updated := now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS lq_schedule_set_updated ON lq_schedules;
CREATE TRIGGER lq_schedule_set_updated
    BEFORE UPDATE
    ON lq_schedules
    FOR EACH ROW
EXECUTE FUNCTION set_lq_schedule_updated_if_changed();


CREATE TABLE IF NOT EXISTS lq_schedule_leader
(
    key         VARCHAR(24) PRIMARY KEY,
    leader_id   VARCHAR NOT NULL,
    valid_until TIMESTAMPTZ
);

