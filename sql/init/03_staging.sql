-- STAGING LAYER ----------------------------------------------------------
-- Rows from raw that could be parsed into proper types. Each row keeps its
-- raw key (source_file, source_line_no) so any value can be traced back.
-- Sensor columns are nullable: a reading with a missing light value is still
-- a usable temperature reading (the profile found 92,975 such rows).

CREATE TABLE IF NOT EXISTS staging.sensor_readings (
    source_file     TEXT             NOT NULL,
    source_line_no  BIGINT           NOT NULL,
    batch_id        TEXT             NOT NULL,
    -- No time zone in the source; the lab's local time is assumed and stored
    -- as TIMESTAMP (without time zone) rather than inventing an offset.
    reading_ts      TIMESTAMP        NOT NULL,
    epoch           INTEGER          NOT NULL,
    moteid          INTEGER          NOT NULL,
    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    light           DOUBLE PRECISION,
    voltage         DOUBLE PRECISION,
    staged_at       TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (source_file, source_line_no)
);
CREATE INDEX IF NOT EXISTS ix_stg_readings_batch ON staging.sensor_readings (batch_id);

-- Rows that could not be staged, with the reason. The original values stay in raw.
CREATE TABLE IF NOT EXISTS staging.rejects (
    source_file     TEXT        NOT NULL,
    source_line_no  BIGINT      NOT NULL,
    batch_id        TEXT        NOT NULL,
    reject_reason   TEXT        NOT NULL,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_file, source_line_no)
);
CREATE INDEX IF NOT EXISTS ix_stg_rejects_batch ON staging.rejects (batch_id);

CREATE TABLE IF NOT EXISTS staging.mote_locs (
    moteid INTEGER PRIMARY KEY,
    x      DOUBLE PRECISION NOT NULL,
    y      DOUBLE PRECISION NOT NULL
);
