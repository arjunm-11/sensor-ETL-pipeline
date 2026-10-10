-- RAW LAYER --------------------------------------------------------------
-- Each source line is stored exactly as written (minus the trailing newline).
-- Splitting into fields is a parsing decision (empty fields are encoded as
-- two consecutive spaces), so it happens in staging, where it is tested and
-- where failures can be recorded as rejects. Raw can therefore never fail to
-- load because of a bad value, and is never updated after insert.

-- One row per load batch. A batch is a fixed-size line range of one file
-- (e.g. lines 1-500000); batch_id is derived from that range, so the same
-- lines always map to the same batch_id.
CREATE TABLE IF NOT EXISTS raw.load_batches (
    batch_id     TEXT PRIMARY KEY,          -- e.g. 'data.txt.gz:000000001-000500000'
    source_file  TEXT        NOT NULL,
    line_start   BIGINT      NOT NULL,      -- 1-based, inclusive (grid start)
    line_end     BIGINT      NOT NULL,      -- last line actually present in the file for this batch
    rows_loaded  BIGINT      NOT NULL,      -- rows in raw for this batch after the load
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()  -- most recent load attempt of this batch
);

CREATE TABLE IF NOT EXISTS raw.sensor_readings (
    source_file     TEXT        NOT NULL,
    source_line_no  BIGINT      NOT NULL,   -- 1-based line number in the source file
    batch_id        TEXT        NOT NULL REFERENCES raw.load_batches (batch_id),
    raw_line        TEXT        NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A physical line of a file is loaded at most once. Inserts use
    -- ON CONFLICT DO NOTHING against this key, which makes reruns no-ops.
    PRIMARY KEY (source_file, source_line_no)
);
CREATE INDEX IF NOT EXISTS ix_raw_readings_batch ON raw.sensor_readings (batch_id);

-- Reference files, same idea: line number + line as-is.
CREATE TABLE IF NOT EXISTS raw.mote_locs (
    source_line_no BIGINT PRIMARY KEY,
    raw_line       TEXT        NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.connectivity (
    source_line_no BIGINT PRIMARY KEY,
    raw_line       TEXT        NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
