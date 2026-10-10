-- Rebuild clean.sensor_readings from staging.sensor_readings.
--
-- Executed by pipeline/clean.py inside ONE transaction, together with the
-- post-condition checks; if any of them fails, everything rolls back and
-- clean keeps its previous contents.
--
-- Why a full rebuild rather than per batch: the file is not time-ordered,
-- so every 500K-line batch spans 34-38 of the 38 days. Deduplication, spike
-- detection and the epoch consensus all compare a reading with its
-- neighbours in time, and those neighbours can be in any batch.
--
-- Thresholds come from the Phase 3 investigation (DECISIONS.md, D16-D21).

TRUNCATE clean.sensor_readings;

INSERT INTO clean.sensor_readings (
    source_file, source_line_no, batch_id, reading_ts, epoch, moteid,
    temperature, humidity, light, voltage,
    is_known_mote, temperature_ok, humidity_ok, light_ok, voltage_ok, dq_flags)
WITH
-- 1. DEDUPLICATION --------------------------------------------------------
-- A true duplicate is the same packet received twice: same mote, same epoch,
-- identical sensor values, arriving within 60 s. (moteid, epoch) alone is NOT
-- a key: the 16-bit epoch counter wraps, so the same pair recurs ~12.7 days
-- later with different values. PARTITION BY treats NULLs as equal, so two
-- copies that both lack light still match.
ranked AS (
    SELECT s.*,
           lag(reading_ts) OVER (
               PARTITION BY moteid, epoch, temperature, humidity, light, voltage
               ORDER BY reading_ts, source_line_no) AS prev_copy_ts
    FROM staging.sensor_readings s
),
deduped AS (
    -- Keep the first copy; drop any copy that follows another within 60 s.
    SELECT * FROM ranked
    WHERE prev_copy_ts IS NULL OR reading_ts - prev_copy_ts > interval '60 seconds'
),

-- 2. EPOCH CONSENSUS ------------------------------------------------------
-- Epoch is a network-wide sample counter, so at any moment healthy motes agree
-- on its value. The median epoch per hour is that consensus; a reading far
-- from it comes from a mote that rebooted (counter restarted near 0) or has a
-- wrong timestamp.
consensus AS (
    SELECT date_trunc('hour', reading_ts) AS hr,
           percentile_disc(0.5) WITHIN GROUP (ORDER BY epoch) AS med_epoch
    FROM deduped
    GROUP BY 1
),

-- 3. ROW-LEVEL RULES ------------------------------------------------------
base AS (
    SELECT d.source_file, d.source_line_no, d.batch_id, d.reading_ts, d.epoch, d.moteid,
           d.temperature, d.humidity, d.light, d.voltage,
           l.moteid IS NOT NULL AS is_known_mote,
           -- Distance on a circular 16-bit counter: maps the difference into
           -- [-32768, 32767] so 65530 vs 5 counts as 11 ticks apart, not 65525.
           -- (The double "% 65536" is because Postgres' % keeps the sign of the dividend.)
           abs((((d.epoch - c.med_epoch) + 32768) % 65536 + 65536) % 65536 - 32768) AS epoch_dev,
           -- Indoor lab. Readings > 50 C occur only below 2.4 V (sensor
           -- drift / stuck value 122.153); < 0 C is 539 rows, none physical.
           d.temperature NOT BETWEEN 0 AND 50       AS temp_oor,
           -- Relative humidity is a percentage. Stuck values -3.91901 and -4.
           d.humidity    NOT BETWEEN 0 AND 100      AS hum_oor,
           -- No negatives in the data; 1847.36 is the sensor's saturation
           -- maximum (bright light), so it is a real reading, not an error.
           d.light < 0                              AS light_oor,
           -- Healthy motes report 2.3-3.19 V; dying ones down to ~1.86 V.
           -- 7 readings < 1.8 V (down to 0.009) and one 18.56 V are glitches.
           d.voltage NOT BETWEEN 1.8 AND 3.3        AS volt_oor,
           -- Below 2.30 V the median temperature is the stuck 122.153 value
           -- and only 5.9% of readings even look plausible: temperature and
           -- humidity from such a mote cannot be trusted.
           d.voltage < 2.3                          AS low_battery
    FROM deduped d
    JOIN consensus c ON c.hr = date_trunc('hour', d.reading_ts)
    LEFT JOIN staging.mote_locs l ON l.moteid = d.moteid
),

-- 4. SPIKES (contextual outliers) -----------------------------------------
-- Among temperature readings that passed the rules above, flag a reading
-- that differs by > 5 C from BOTH its previous and next reading of the same
-- mote, in the same direction, with both neighbours within 120 s. That is an
-- isolated blip. A genuine step change differs from only one neighbour and
-- is not flagged. Normal consecutive change: median 0.01 C, p99.99 2.5 C.
trusted AS (
    SELECT source_file, source_line_no, temperature, reading_ts,
           lag(temperature)  OVER w AS prev_t, lag(reading_ts)  OVER w AS prev_ts,
           lead(temperature) OVER w AS next_t, lead(reading_ts) OVER w AS next_ts
    FROM base
    WHERE temperature IS NOT NULL AND NOT temp_oor AND NOT coalesce(low_battery, false)
    WINDOW w AS (PARTITION BY moteid ORDER BY reading_ts, source_line_no)
),
spikes AS (
    SELECT source_file, source_line_no
    FROM trusted
    WHERE reading_ts - prev_ts <= interval '120 seconds'
      AND next_ts - reading_ts <= interval '120 seconds'
      AND abs(temperature - prev_t) > 5
      AND abs(temperature - next_t) > 5
      AND sign(temperature - prev_t) = sign(temperature - next_t)
)

SELECT b.source_file, b.source_line_no, b.batch_id, b.reading_ts, b.epoch, b.moteid,
       b.temperature, b.humidity, b.light, b.voltage,
       b.is_known_mote,
       -- coalesce(..., false): a NULL comparison (missing value) means "not flagged" for
       -- that rule; the *_missing flag and the IS NOT NULL test cover missing values.
       b.temperature IS NOT NULL AND NOT b.temp_oor AND NOT coalesce(b.low_battery, false)
           AND sp.source_line_no IS NULL                                AS temperature_ok,
       b.humidity IS NOT NULL AND NOT b.hum_oor AND NOT coalesce(b.low_battery, false) AS humidity_ok,
       b.light IS NOT NULL AND NOT b.light_oor                          AS light_ok,
       b.voltage IS NOT NULL AND NOT b.volt_oor                         AS voltage_ok,
       -- array_remove(..., NULL) drops the CASEs that didn't fire.
       array_remove(ARRAY[
           CASE WHEN NOT b.is_known_mote               THEN 'unknown_mote' END,
           CASE WHEN b.temperature IS NULL             THEN 'temperature_missing' END,
           CASE WHEN b.humidity IS NULL                THEN 'humidity_missing' END,
           CASE WHEN b.light IS NULL                   THEN 'light_missing' END,
           CASE WHEN b.voltage IS NULL                 THEN 'voltage_missing' END,
           CASE WHEN b.temp_oor                        THEN 'temperature_out_of_range' END,
           CASE WHEN b.hum_oor                         THEN 'humidity_out_of_range' END,
           CASE WHEN b.light_oor                       THEN 'light_out_of_range' END,
           CASE WHEN b.volt_oor                        THEN 'voltage_out_of_range' END,
           CASE WHEN b.low_battery                     THEN 'low_battery' END,
           CASE WHEN sp.source_line_no IS NOT NULL     THEN 'temperature_spike' END,
           -- 240 ticks = 2 h. 99.2% of readings are within 1 h of consensus.
           CASE WHEN b.epoch_dev > 240                 THEN 'epoch_inconsistent' END
       ], NULL) AS dq_flags
FROM base b
LEFT JOIN spikes sp USING (source_file, source_line_no);
