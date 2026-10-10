-- CLEAN LAYER ------------------------------------------------------------
-- Staging minus true duplicates, plus data-quality flags. Nothing is deleted
-- for being implausible: every reading stays, and the *_ok columns say
-- whether each value is fit for analytics. Rules: sql/transform/clean.sql.

CREATE TABLE IF NOT EXISTS clean.sensor_readings (
    source_file     TEXT             NOT NULL,
    source_line_no  BIGINT           NOT NULL,
    batch_id        TEXT             NOT NULL,
    reading_ts      TIMESTAMP        NOT NULL,
    epoch           INTEGER          NOT NULL,
    moteid          INTEGER          NOT NULL,
    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    light           DOUBLE PRECISION,
    voltage         DOUBLE PRECISION,
    is_known_mote   BOOLEAN          NOT NULL,  -- moteid present in mote_locs.txt
    -- Per-value usability. Marts aggregate a value only when its flag is true,
    -- so a reading with a broken temperature can still contribute its light.
    temperature_ok  BOOLEAN          NOT NULL,
    humidity_ok     BOOLEAN          NOT NULL,
    light_ok        BOOLEAN          NOT NULL,
    voltage_ok      BOOLEAN          NOT NULL,
    -- Names of every rule this row tripped; '{}' = clean row.
    dq_flags        TEXT[]           NOT NULL,
    cleaned_at      TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (source_file, source_line_no)
);
CREATE INDEX IF NOT EXISTS ix_clean_mote_ts ON clean.sensor_readings (moteid, reading_ts);

-- When did the deployment stop? Not max(reading_ts): after ~2004-04-01 23:00
-- only a trickle remains (mostly unregistered motes 56/58), so "silent
-- relative to the last timestamp" would call every mote silent. Instead:
-- the last hour in which at least 10% of registered motes reported. The
-- result is stable: thresholds 5-30% all give 2004-04-01 23:00-2004-04-02 01:00.
-- Used by the silent_motes check and the mote health mart.
CREATE OR REPLACE VIEW clean.v_network_end AS
WITH hourly AS (
    SELECT date_trunc('hour', reading_ts) AS hr, count(DISTINCT moteid) AS motes
    FROM clean.sensor_readings
    WHERE is_known_mote
    GROUP BY 1
)
SELECT max(hr) + interval '1 hour' AS network_end   -- end of that hour
FROM hourly
WHERE motes >= 0.10 * (SELECT count(*) FROM staging.mote_locs);
