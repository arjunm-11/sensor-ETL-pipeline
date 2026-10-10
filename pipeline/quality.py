"""
Data-quality checks and their recording in audit.dq_results.

Two kinds of check:
  severity 'error'  a failure means the pipeline's own output is wrong or
                    unsafe to build on (e.g. rows lost). The task raises
                    DataQualityError, which fails the Airflow task.
  severity 'warn'   a failure means the *source data* is worse than our
                    tolerance. It's recorded and reported, and the pipeline
                    continues, because the bad values are flagged and excluded
                    from marts (not dropped).

Thresholds are the maximum failing fraction that still counts as a pass.
They are policy choices (how much bad data we tolerate before someone
should look), NOT values fitted to make this dataset pass. Several checks
fail on this dataset, and that's an accurate description of it.
"""
import logging
from dataclasses import dataclass

from . import db

log = logging.getLogger(__name__)


class DataQualityError(Exception):
    pass


@dataclass
class CheckResult:
    check_name: str
    layer: str
    severity: str
    rows_checked: int
    rows_failed: int
    threshold: float
    description: str
    batch_id: str = "ALL"

    @property
    def passed(self) -> bool:
        if self.rows_checked == 0:
            return self.rows_failed == 0
        return self.rows_failed / self.rows_checked <= self.threshold


# Row flags written by sql/transform/clean.sql -> (warn threshold, description).
FLAG_CHECKS = {
    "unknown_mote":             (0.01,  "mote id not in mote_locs.txt (ids 1-54)"),
    "temperature_missing":      (0.01,  "temperature field empty"),
    "humidity_missing":         (0.01,  "humidity field empty"),
    "light_missing":            (0.05,  "light field empty"),
    "voltage_missing":          (0.01,  "voltage field empty"),
    "temperature_out_of_range": (0.05,  "temperature outside 0-50 C (indoor lab)"),
    "humidity_out_of_range":    (0.05,  "humidity outside 0-100 %"),
    "light_out_of_range":       (0.0,   "light below 0 lux"),
    "voltage_out_of_range":     (0.001, "voltage outside 1.8-3.3 V"),
    "low_battery":              (0.05,  "voltage below 2.30 V: temperature/humidity unreliable"),
    "temperature_spike":        (0.001, "isolated temperature blip > 5 C vs both neighbours within 120 s"),
    "epoch_inconsistent":       (0.01,  "epoch > 2 h (240 ticks) from the network's hourly consensus"),
}

MAX_REJECT_RATE = 0.01        # > 1% unparseable lines suggests a format change, not noise
MAX_DUPLICATE_RATE = 0.001
MAX_SILENT_MOTE_SHARE = 0.10
SILENT_AFTER = "24 hours"
MAX_OUTAGE_HOUR_SHARE = 0.01


def record(run_id: str, results: list[CheckResult]) -> None:
    """Upsert results, so an Airflow retry of the same run overwrites instead of duplicating."""
    with db.transaction() as cur:
        for r in results:
            cur.execute(
                """INSERT INTO audit.dq_results
                     (run_id, batch_id, check_name, layer, severity, rows_checked, rows_failed,
                      threshold, passed, description)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (run_id, check_name, batch_id) DO UPDATE SET
                     layer = EXCLUDED.layer, severity = EXCLUDED.severity,
                     rows_checked = EXCLUDED.rows_checked, rows_failed = EXCLUDED.rows_failed,
                     threshold = EXCLUDED.threshold, passed = EXCLUDED.passed,
                     description = EXCLUDED.description, checked_at = now()""",
                (run_id, r.batch_id, r.check_name, r.layer, r.severity, r.rows_checked,
                 r.rows_failed, r.threshold, r.passed, r.description),
            )
    for r in results:
        log.log(logging.INFO if r.passed else logging.WARNING,
                "%-26s %-5s %-4s failed %s/%s (batch %s)", r.check_name, r.severity,
                "PASS" if r.passed else "FAIL", f"{r.rows_failed:,}", f"{r.rows_checked:,}", r.batch_id)


def raise_on_errors(results: list[CheckResult]) -> None:
    bad = [r for r in results if r.severity == "error" and not r.passed]
    if bad:
        raise DataQualityError("; ".join(f"{r.check_name} ({r.batch_id}): {r.rows_failed} failed" for r in bad))


# ---------------------------------------------------------------- validate ---
def staging_checks(cur, batch_ids: list[str]) -> list[CheckResult]:
    """Gate before building clean: is staging complete and in the expected format?"""
    results = []
    for b in batch_ids:
        cur.execute(
            """SELECT (SELECT count(*) FROM raw.sensor_readings     WHERE batch_id = %(b)s),
                      (SELECT count(*) FROM staging.sensor_readings WHERE batch_id = %(b)s),
                      (SELECT count(*) FROM staging.rejects         WHERE batch_id = %(b)s)""",
            {"b": b},
        )
        n_raw, n_staged, n_rej = cur.fetchone()
        results += [
            CheckResult("stage_reconciliation", "staging", "error", n_raw, abs(n_raw - n_staged - n_rej), 0.0,
                        "every raw line is either staged or rejected", b),
            CheckResult("reject_rate", "staging", "error", n_raw, n_rej, MAX_REJECT_RATE,
                        "lines that could not be parsed", b),
        ]
    cur.execute("SELECT count(*) FROM staging.mote_locs")
    n_locs = cur.fetchone()[0]
    # Without the mote registry every reading would be flagged unknown_mote.
    results.append(CheckResult("mote_locs_loaded", "staging", "error", 1, 0 if n_locs else 1, 0.0,
                               f"mote location reference table is populated ({n_locs} motes)"))
    return results


def validate_staging(batch_ids: list[str], run_id: str) -> list[CheckResult]:
    with db.transaction() as cur:
        results = staging_checks(cur, batch_ids)
    record(run_id, results)          # recorded before raising, so failures are visible in audit
    raise_on_errors(results)
    return results


# ------------------------------------------------------------------- clean ---
def clean_checks(cur) -> list[CheckResult]:
    """Run against the freshly built clean table, inside the build transaction."""
    results = []
    cur.execute("SELECT (SELECT count(*) FROM staging.sensor_readings), (SELECT count(*) FROM clean.sensor_readings)")
    n_staged, n_clean = cur.fetchone()

    # Post-condition 1: nothing lost. Every staged row missing from clean must
    # have an identical copy in clean that arrived within 60 s.
    cur.execute(
        """SELECT count(*) FROM staging.sensor_readings s
           WHERE NOT EXISTS (SELECT 1 FROM clean.sensor_readings c
                             WHERE c.source_file = s.source_file AND c.source_line_no = s.source_line_no)
             AND NOT EXISTS (SELECT 1 FROM clean.sensor_readings c
                             WHERE c.moteid = s.moteid AND c.epoch = s.epoch
                               AND c.temperature IS NOT DISTINCT FROM s.temperature
                               AND c.humidity    IS NOT DISTINCT FROM s.humidity
                               AND c.light       IS NOT DISTINCT FROM s.light
                               AND c.voltage     IS NOT DISTINCT FROM s.voltage
                               AND abs(extract(epoch FROM c.reading_ts - s.reading_ts)) <= 60)""")
    results.append(CheckResult("no_rows_lost", "clean", "error", n_staged, cur.fetchone()[0], 0.0,
                               "staged rows absent from clean are all duplicates of a kept row"))

    # Post-condition 2: dedup worked; no duplicate pair remains in clean.
    cur.execute(
        """SELECT count(*) FROM (
             SELECT reading_ts - lag(reading_ts) OVER (
                      PARTITION BY moteid, epoch, temperature, humidity, light, voltage
                      ORDER BY reading_ts) AS gap
             FROM clean.sensor_readings) x
           WHERE gap <= interval '60 seconds'""")
    results.append(CheckResult("no_duplicates_remaining", "clean", "error", n_clean, cur.fetchone()[0], 0.0,
                               "no two clean rows are the same packet (mote, epoch, values, within 60 s)"))

    results.append(CheckResult("duplicates_removed", "clean", "warn", n_staged, n_staged - n_clean,
                               MAX_DUPLICATE_RATE, "same packet received more than once"))

    # Flag counts. unnest() turns each row's flag array into one row per flag.
    cur.execute("SELECT f, count(*) FROM clean.sensor_readings, unnest(dq_flags) AS f GROUP BY f")
    counts = dict(cur.fetchall())
    for flag, (threshold, desc) in FLAG_CHECKS.items():
        # Rules that found nothing are recorded too: a pass is a result.
        results.append(CheckResult(flag, "clean", "warn", n_clean, counts.pop(flag, 0), threshold, desc))
    for flag, n in counts.items():   # flag emitted by clean.sql but not registered above
        log.warning("unregistered flag %s", flag)
        results.append(CheckResult(flag, "clean", "warn", n_clean, n, 0.0, "unregistered flag"))

    # Mote-level: known motes that stopped reporting more than 24 h before the
    # network as a whole stopped (clean.v_network_end), or never reported at all.
    cur.execute(
        f"""WITH last_seen AS (SELECT moteid, max(reading_ts) AS last_ts FROM clean.sensor_readings GROUP BY 1)
            SELECT count(*),
                   count(*) FILTER (WHERE ls.last_ts IS NULL
                       OR ls.last_ts < (SELECT network_end FROM clean.v_network_end) - interval '{SILENT_AFTER}')
            FROM staging.mote_locs l LEFT JOIN last_seen ls USING (moteid)""")
    n_motes, n_silent = cur.fetchone()
    results.append(CheckResult("silent_motes", "clean", "warn", n_motes, n_silent, MAX_SILENT_MOTE_SHARE,
                               f"known motes silent for more than {SILENT_AFTER} before the network stopped "
                               "(units: motes)"))

    # Network-wide completeness: hours (while the network was running) in which
    # NO registered mote reported. Separates base-station/logging outages from
    # individual mote failures; e.g. 2004-03-10 09:00 to 03-11 09:00 is empty
    # for all motes at once.
    cur.execute(
        """WITH hours AS (
               SELECT generate_series(
                   date_trunc('hour', (SELECT min(reading_ts) FROM clean.sensor_readings WHERE is_known_mote)),
                   (SELECT network_end FROM clean.v_network_end) - interval '1 hour',
                   interval '1 hour') AS hr),
           seen AS (SELECT DISTINCT date_trunc('hour', reading_ts) AS hr
                    FROM clean.sensor_readings WHERE is_known_mote)
           SELECT count(*), count(*) FILTER (WHERE s.hr IS NULL)
           FROM hours h LEFT JOIN seen s USING (hr)""")
    n_hours, n_empty = cur.fetchone()
    results.append(CheckResult("network_outage_hours", "clean", "warn", n_hours, n_empty, MAX_OUTAGE_HOUR_SHARE,
                               "hours while the network was running with zero readings from any registered mote "
                               "(units: hours)"))
    return results


# ------------------------------------------------------------------- marts ---
def marts_checks(cur) -> list[CheckResult]:
    """Reconcile marts against clean, inside the marts build transaction."""
    cur.execute(
        """SELECT (SELECT count(*) FROM clean.sensor_readings WHERE is_known_mote),
                  (SELECT coalesce(sum(reading_count), 0) FROM marts.mote_daily),
                  (SELECT coalesce(sum(cardinality(dq_flags)), 0) FROM clean.sensor_readings),
                  (SELECT coalesce(sum(readings), 0) FROM marts.anomaly_counts),
                  (SELECT count(*) FROM staging.mote_locs),
                  (SELECT count(*) FROM marts.mote_health),
                  (SELECT count(*) FROM marts.mote_daily),
                  (SELECT count(DISTINCT day) FROM marts.mote_daily)""")
    n_known, n_daily, n_flags, n_anom, n_motes, n_health, n_daily_rows, n_days = cur.fetchone()
    return [
        CheckResult("mote_daily_reconciles", "marts", "error", n_known, abs(n_known - n_daily), 0.0,
                    "sum of mote_daily.reading_count equals registered-mote rows in clean"),
        CheckResult("mote_daily_grid_complete", "marts", "error", n_motes * n_days,
                    abs(n_motes * n_days - n_daily_rows), 0.0,
                    "one mote_daily row per registered mote per day, including empty days"),
        CheckResult("anomaly_counts_reconcile", "marts", "error", n_flags, abs(n_flags - n_anom), 0.0,
                    "sum of anomaly_counts equals total flags in clean"),
        CheckResult("mote_health_complete", "marts", "error", n_motes, abs(n_motes - n_health), 0.0,
                    "one mote_health row per registered mote"),
    ]
