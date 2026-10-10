-- Rebuild all marts from clean.sensor_readings.
-- Executed by pipeline/marts.py in ONE transaction together with the
-- reconciliation checks, so a failed check leaves the previous marts intact.

-- Sampling schedule: one reading per mote per epoch tick. The source docs
-- say 31 s, but a regression of the network's hourly median epoch on time
-- gives 30.001 s/tick (r^2 = 0.9999999), so 30 s is used here.
-- Literal 30.0 appears below; keep it in sync with DECISIONS.md D26.

TRUNCATE marts.mote_daily, marts.mote_health, marts.anomaly_counts;

-- ---------------------------------------------------------------------------
-- 1. marts.mote_daily
-- ---------------------------------------------------------------------------
INSERT INTO marts.mote_daily
WITH known AS (
    SELECT * FROM clean.sensor_readings WHERE is_known_mote
),
bounds AS (
    SELECT (SELECT min(reading_ts) FROM known)                AS net_start,
           (SELECT network_end FROM clean.v_network_end)      AS net_end,
           (SELECT max(reading_ts) FROM known)                AS last_reading
),
days AS (
    -- Every calendar day from the first to the last registered-mote reading.
    SELECT generate_series(date_trunc('day', net_start), date_trunc('day', last_reading),
                           interval '1 day')::date AS day
    FROM bounds
),
day_expected AS (
    -- Seconds of this day during which the network was running, / 30 s.
    -- Days after network_end get 0 expected readings (delivery_rate NULL).
    SELECT d.day,
           greatest(0, extract(epoch FROM
               least(d.day + interval '1 day', b.net_end) - greatest(d.day::timestamp, b.net_start)
           )) / 30.0 AS expected
    FROM days d CROSS JOIN bounds b
),
agg AS (
    SELECT moteid, reading_ts::date AS day,
           count(*)                                         AS reading_count,
           count(*) FILTER (WHERE dq_flags <> '{}')         AS flagged_count,
           -- FILTER restricts each aggregate to usable values only.
           count(*)         FILTER (WHERE temperature_ok)   AS temperature_n,
           avg(temperature) FILTER (WHERE temperature_ok)   AS temperature_avg,
           min(temperature) FILTER (WHERE temperature_ok)   AS temperature_min,
           max(temperature) FILTER (WHERE temperature_ok)   AS temperature_max,
           count(*)         FILTER (WHERE humidity_ok)      AS humidity_n,
           avg(humidity)    FILTER (WHERE humidity_ok)      AS humidity_avg,
           min(humidity)    FILTER (WHERE humidity_ok)      AS humidity_min,
           max(humidity)    FILTER (WHERE humidity_ok)      AS humidity_max,
           count(*)         FILTER (WHERE light_ok)         AS light_n,
           avg(light)       FILTER (WHERE light_ok)         AS light_avg,
           min(light)       FILTER (WHERE light_ok)         AS light_min,
           max(light)       FILTER (WHERE light_ok)         AS light_max,
           count(*)         FILTER (WHERE voltage_ok)       AS voltage_n,
           avg(voltage)     FILTER (WHERE voltage_ok)       AS voltage_avg,
           min(voltage)     FILTER (WHERE voltage_ok)       AS voltage_min,
           max(voltage)     FILTER (WHERE voltage_ok)       AS voltage_max
    FROM known
    GROUP BY 1, 2
)
SELECT l.moteid, e.day,
       coalesce(a.reading_count, 0), coalesce(a.flagged_count, 0),
       round(e.expected::numeric, 1),
       CASE WHEN e.expected > 0 THEN round(coalesce(a.reading_count, 0) / e.expected::numeric, 4) END,
       coalesce(a.temperature_n, 0), a.temperature_avg, a.temperature_min, a.temperature_max,
       coalesce(a.humidity_n, 0),    a.humidity_avg,    a.humidity_min,    a.humidity_max,
       coalesce(a.light_n, 0),       a.light_avg,       a.light_min,       a.light_max,
       coalesce(a.voltage_n, 0),     a.voltage_avg,     a.voltage_min,     a.voltage_max
-- Dense grid: every registered mote x every day, LEFT JOIN the readings.
FROM staging.mote_locs l
CROSS JOIN day_expected e
LEFT JOIN agg a ON a.moteid = l.moteid AND a.day = e.day;

-- ---------------------------------------------------------------------------
-- 2. marts.mote_health
-- ---------------------------------------------------------------------------
INSERT INTO marts.mote_health (
    moteid, x, y, status, first_seen, last_seen, hours_silent_before_end, total_readings,
    delivery_rate, delivery_rate_while_alive, gaps_over_10min, gaps_over_1h, longest_gap_hours,
    voltage_first_day_avg, voltage_last_day_avg, voltage_trend_v_per_day, low_battery_since,
    temperature_ok_pct)
WITH bounds AS (
    SELECT (SELECT min(reading_ts) FROM clean.sensor_readings WHERE is_known_mote) AS net_start,
           (SELECT network_end FROM clean.v_network_end)                          AS net_end
),
gaps AS (
    -- Time since the previous reading of the same mote. Any received reading
    -- counts (flagged or not): the question is "did the radio deliver?".
    SELECT moteid,
           reading_ts - lag(reading_ts) OVER (PARTITION BY moteid ORDER BY reading_ts) AS gap
    FROM clean.sensor_readings
    WHERE is_known_mote
),
gap_stats AS (
    SELECT moteid,
           count(*) FILTER (WHERE gap > interval '10 minutes') AS gaps_over_10min,
           count(*) FILTER (WHERE gap > interval '1 hour')     AS gaps_over_1h,
           extract(epoch FROM max(gap)) / 3600                 AS longest_gap_hours
    FROM gaps GROUP BY 1
),
per_mote AS (
    SELECT moteid, min(reading_ts) AS first_seen, max(reading_ts) AS last_seen, count(*) AS total_readings,
           avg(temperature_ok::int) AS temperature_ok_share
    FROM clean.sensor_readings WHERE is_known_mote GROUP BY 1
),
volt AS (
    -- Battery trend from the daily mart: slope of daily average voltage per
    -- day (regr_slope = least-squares fit), first/last day averages, and the
    -- first day the average fell below the 2.30 V low-battery threshold.
    SELECT moteid,
           regr_slope(voltage_avg, day - DATE '2004-01-01')           AS trend,
           (array_agg(voltage_avg ORDER BY day))[1]                   AS first_avg,
           (array_agg(voltage_avg ORDER BY day DESC))[1]              AS last_avg,
           min(day) FILTER (WHERE voltage_avg < 2.3)                  AS low_since
    FROM marts.mote_daily
    WHERE voltage_n > 0
    GROUP BY 1
)
SELECT l.moteid, l.x, l.y,
       CASE WHEN p.moteid IS NULL THEN 'never_reported'
            -- Same 24 h rule as the silent_motes check (pipeline/quality.py).
            WHEN p.last_seen < b.net_end - interval '24 hours' THEN 'dead'
            ELSE 'alive_at_end' END,
       p.first_seen, p.last_seen,
       round((extract(epoch FROM greatest(b.net_end - p.last_seen, interval '0')) / 3600)::numeric, 1),
       coalesce(p.total_readings, 0),
       round(coalesce(p.total_readings, 0) / (extract(epoch FROM b.net_end - b.net_start) / 30.0)::numeric, 4),
       -- nullif guards a mote with a single reading (zero-length active span).
       round(p.total_readings / nullif(extract(epoch FROM p.last_seen - p.first_seen) / 30.0, 0)::numeric, 4),
       coalesce(g.gaps_over_10min, 0), coalesce(g.gaps_over_1h, 0), round(g.longest_gap_hours::numeric, 2),
       v.first_avg, v.last_avg, v.trend, v.low_since,
       round(p.temperature_ok_share::numeric, 4)
FROM staging.mote_locs l
CROSS JOIN bounds b
LEFT JOIN per_mote p  ON p.moteid = l.moteid
LEFT JOIN gap_stats g ON g.moteid = l.moteid
LEFT JOIN volt v      ON v.moteid = l.moteid;

-- ---------------------------------------------------------------------------
-- 3. marts.anomaly_counts
-- ---------------------------------------------------------------------------
INSERT INTO marts.anomaly_counts (day, moteid, flag, readings)
SELECT reading_ts::date, moteid, f, count(*)
FROM clean.sensor_readings, unnest(dq_flags) AS f   -- one row per (reading, flag)
GROUP BY 1, 2, 3;
