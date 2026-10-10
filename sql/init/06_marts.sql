-- MARTS ------------------------------------------------------------------
-- Built from clean.sensor_readings by sql/transform/marts.sql (full rebuild).
-- mote_daily and mote_health cover the 54 registered motes only (see
-- DECISIONS.md D27); anomaly_counts covers every mote id, because an
-- unregistered id is itself an anomaly worth counting.

-- One row per registered mote per day, including days with zero readings
-- (dense grid), so a dead mote shows up as delivery_rate = 0 instead of
-- silently disappearing from the table.
CREATE TABLE IF NOT EXISTS marts.mote_daily (
    moteid             INTEGER  NOT NULL,
    day                DATE     NOT NULL,
    reading_count      INTEGER  NOT NULL,   -- all clean readings received, flagged or not
    flagged_count      INTEGER  NOT NULL,   -- readings with at least one dq flag
    -- Readings the schedule says we should have got: one per 30 s epoch tick
    -- for the part of this day when the network was running.
    expected_readings  NUMERIC  NOT NULL,
    delivery_rate      NUMERIC,             -- reading_count / expected; NULL when nothing was expected
    -- Aggregates use only values whose *_ok flag is true; *_n = how many.
    temperature_n      INTEGER  NOT NULL,
    temperature_avg    DOUBLE PRECISION,
    temperature_min    DOUBLE PRECISION,
    temperature_max    DOUBLE PRECISION,
    humidity_n         INTEGER  NOT NULL,
    humidity_avg       DOUBLE PRECISION,
    humidity_min       DOUBLE PRECISION,
    humidity_max       DOUBLE PRECISION,
    light_n            INTEGER  NOT NULL,
    light_avg          DOUBLE PRECISION,
    light_min          DOUBLE PRECISION,
    light_max          DOUBLE PRECISION,
    voltage_n          INTEGER  NOT NULL,
    voltage_avg        DOUBLE PRECISION,
    voltage_min        DOUBLE PRECISION,
    voltage_max        DOUBLE PRECISION,
    built_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (moteid, day)
);

-- One row per registered mote: is it alive, how well did it deliver, how is its battery.
CREATE TABLE IF NOT EXISTS marts.mote_health (
    moteid                     INTEGER PRIMARY KEY,
    x                          DOUBLE PRECISION NOT NULL,
    y                          DOUBLE PRECISION NOT NULL,
    status                     TEXT    NOT NULL CHECK (status IN ('alive_at_end', 'dead', 'never_reported')),
    first_seen                 TIMESTAMP,
    last_seen                  TIMESTAMP,
    hours_silent_before_end    NUMERIC,     -- network_end - last_seen
    total_readings             INTEGER NOT NULL,
    delivery_rate              NUMERIC,     -- vs. the whole network run
    delivery_rate_while_alive  NUMERIC,     -- vs. this mote's own first_seen..last_seen
    gaps_over_10min            INTEGER NOT NULL,
    gaps_over_1h               INTEGER NOT NULL,
    longest_gap_hours          NUMERIC,
    voltage_first_day_avg      DOUBLE PRECISION,
    voltage_last_day_avg       DOUBLE PRECISION,
    voltage_trend_v_per_day    DOUBLE PRECISION,  -- least-squares slope of daily average voltage
    low_battery_since          DATE,        -- first day whose average voltage was < 2.30 V
    temperature_ok_pct         NUMERIC,     -- share of readings with a usable temperature
    built_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Long format (one row per day, mote, flag), so a new rule needs no schema change.
CREATE TABLE IF NOT EXISTS marts.anomaly_counts (
    day        DATE    NOT NULL,
    moteid     INTEGER NOT NULL,
    flag       TEXT    NOT NULL,
    readings   INTEGER NOT NULL,
    built_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (day, moteid, flag)
);
