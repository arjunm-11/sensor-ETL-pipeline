-- AUDIT ------------------------------------------------------------------
-- One row per data-quality check per pipeline run (per batch where relevant).
CREATE TABLE IF NOT EXISTS audit.dq_results (
    run_id       TEXT        NOT NULL,     -- Airflow run id (or a manual id outside Airflow)
    -- 'ALL' for checks that span every batch (PK columns cannot be NULL).
    batch_id     TEXT        NOT NULL DEFAULT 'ALL',
    check_name   TEXT        NOT NULL,
    layer        TEXT        NOT NULL,     -- which layer the check ran against
    -- 'error' = a failure stops the pipeline; 'warn' = recorded and reported only.
    severity     TEXT        NOT NULL CHECK (severity IN ('error', 'warn')),
    rows_checked BIGINT      NOT NULL,     -- for mote-level checks: motes checked
    rows_failed  BIGINT      NOT NULL,
    threshold    DOUBLE PRECISION NOT NULL, -- max failure fraction that still counts as a pass
    passed       BOOLEAN     NOT NULL,
    description  TEXT        NOT NULL,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name, batch_id)
);
