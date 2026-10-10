"""
Per-run markdown data-quality report: reports/dq_report_<run_id>.md

Everything comes from the database: this run's rows in audit.dq_results plus
queries on the layers. Rerunning for the same run_id overwrites the file.
"""
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from . import db

log = logging.getLogger(__name__)

REPORT_DIR = Path(os.environ.get("WSN_REPORT_DIR", "reports"))


def _table(headers: list[str], rows: list) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return "\n".join(out)


def _n(x) -> str:
    return f"{x:,}"


def _pct(a, b) -> str:
    return f"{100 * a / b:.2f}%" if b else "n/a"


def _q(cur, sql, params=None):
    cur.execute(sql, params)
    return cur.fetchall()


def build_report(run_id: str) -> Path:
    with db.transaction() as cur:
        md = [f"# Data quality report: run `{run_id}`", "",
              f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC from `audit.dq_results` "
              "and the pipeline tables.", ""]

        # ---- 1. Row counts through the layers ------------------------------
        (n_raw, n_staged, n_rej, n_clean, n_noflag, n_known, n_batches) = _q(cur, """
            SELECT (SELECT count(*) FROM raw.sensor_readings),
                   (SELECT count(*) FROM staging.sensor_readings),
                   (SELECT count(*) FROM staging.rejects),
                   (SELECT count(*) FROM clean.sensor_readings),
                   (SELECT count(*) FROM clean.sensor_readings WHERE dq_flags = '{}'),
                   (SELECT count(*) FROM clean.sensor_readings WHERE is_known_mote),
                   (SELECT count(*) FROM raw.load_batches)""")[0]
        md += ["## 1. Rows through the layers", "",
               _table(["layer", "rows", "note"], [
                   ["raw", _n(n_raw), f"{n_batches} batches"],
                   ["staging", _n(n_staged), f"{_n(n_rej)} rejected ({_pct(n_rej, n_raw)} of raw)"],
                   ["clean", _n(n_clean), f"{_n(n_staged - n_clean)} duplicates removed"],
                   ["clean, no flags", _n(n_noflag), _pct(n_noflag, n_clean) + " of clean"],
                   ["clean, registered motes (feed marts)", _n(n_known), _pct(n_known, n_clean) + " of clean"],
               ]), ""]

        # ---- 2. Check results ----------------------------------------------
        checks = _q(cur, """
            SELECT layer, check_name, batch_id, severity, rows_checked, rows_failed, threshold, passed, description
            FROM audit.dq_results WHERE run_id = %s
            ORDER BY CASE layer WHEN 'staging' THEN 1 WHEN 'clean' THEN 2 ELSE 3 END, checked_at, check_name, batch_id""",
            (run_id,))
        n_fail_err = sum(1 for c in checks if c[3] == "error" and not c[7])
        n_fail_warn = sum(1 for c in checks if c[3] == "warn" and not c[7])
        md += ["## 2. Check results", "",
               f"{len(checks)} checks recorded for this run: **{n_fail_err} error-severity failures**, "
               f"**{n_fail_warn} warnings**. Error failures stop the pipeline; warnings describe the source "
               "data (flagged values are excluded from marts, not deleted).", ""]
        if not checks:
            md += ["_No checks recorded for this run id._", ""]
        md += [_table(["result", "layer", "check", "batch", "severity", "checked", "failed", "failed %",
                       "threshold", "what it checks"],
                      [["PASS" if p else "**FAIL**", layer, f"`{name}`", "all" if b == "ALL" else f"`{b.split(':')[-1]}`",
                        sev, _n(chk), _n(fail), _pct(fail, chk), f"{thr:.1%}", desc]
                       for layer, name, b, sev, chk, fail, thr, p, desc in checks]), ""]

        # ---- 3. Rejects ----------------------------------------------------
        rej = _q(cur, "SELECT reject_reason, count(*) FROM staging.rejects GROUP BY 1 ORDER BY 2 DESC")
        md += ["## 3. Rejected lines (staging)", "",
               _table(["reason", "lines"], [[r, _n(c)] for r, c in rej]) if rej else "_None._", ""]

        # ---- 4. Flags and the values behind them ----------------------------
        # The most frequent offending values expose stuck-sensor values
        # (e.g. 122.153 C), which a count alone would hide.
        md += ["## 4. What the flagged values are", "",
               "Most frequent values among flagged readings:", ""]
        for flag, col in [("temperature_out_of_range", "temperature"), ("humidity_out_of_range", "humidity"),
                          ("voltage_out_of_range", "voltage")]:
            top = _q(cur, f"""SELECT {col}, count(*) FROM clean.sensor_readings
                              WHERE %s = ANY(dq_flags) GROUP BY 1 ORDER BY 2 DESC LIMIT 3""", (flag,))
            total = _q(cur, "SELECT count(*) FROM clean.sensor_readings WHERE %s = ANY(dq_flags)", (flag,))[0][0]
            md += [f"- `{flag}` ({_n(total)} rows): " +
                   ", ".join(f"`{v}` × {_n(c)} ({_pct(c, total)})" for v, c in top)]
        usable = _q(cur, """SELECT count(*) FILTER (WHERE temperature_ok), count(*) FILTER (WHERE humidity_ok),
                                   count(*) FILTER (WHERE light_ok), count(*) FILTER (WHERE voltage_ok)
                            FROM clean.sensor_readings""")[0]
        md += ["", "Usable values per metric (what the marts aggregate):", "",
               _table(["metric", "usable", "share of clean"],
                      [[m, _n(u), _pct(u, n_clean)] for m, u in
                       zip(["temperature", "humidity", "light", "voltage"], usable)]), ""]

        # ---- 5. Mote health ------------------------------------------------
        status = _q(cur, "SELECT status, count(*) FROM marts.mote_health GROUP BY 1 ORDER BY 1")
        dead = _q(cur, """
            SELECT moteid, last_seen::date, hours_silent_before_end, delivery_rate_while_alive,
                   round(voltage_last_day_avg::numeric, 3), low_battery_since
            FROM marts.mote_health WHERE status <> 'alive_at_end' ORDER BY last_seen NULLS FIRST""")
        net_end = _q(cur, "SELECT network_end FROM clean.v_network_end")[0][0]
        md += ["## 5. Mote health (registered motes)", "",
               f"Network considered stopped at **{net_end}** (last hour with ≥ 10% of registered motes "
               "reporting). A mote is `dead` if silent for more than 24 h before that.", "",
               _table(["status", "motes"], status), "",
               "Dead motes. *Low battery since* is empty when the daily average voltage never fell below 2.30 V, "
               "meaning the mote stopped for a reason other than a flat battery:", "",
               _table(["mote", "last seen", "hours silent before end", "delivery while alive",
                       "last-day avg V", "low battery since"], dead) if dead else "_None._", ""]

        # ---- 6. Network-wide outages ---------------------------------------
        # Gaps-and-islands: consecutive empty hours share the same value of
        # (hour - row_number * 1h), which groups them into one outage.
        outages = _q(cur, """
            WITH hours AS (
                SELECT generate_series(
                    date_trunc('hour', (SELECT min(reading_ts) FROM clean.sensor_readings WHERE is_known_mote)),
                    (SELECT network_end FROM clean.v_network_end) - interval '1 hour',
                    interval '1 hour') AS hr),
            empty AS (
                SELECT hr FROM hours
                WHERE hr NOT IN (SELECT DISTINCT date_trunc('hour', reading_ts)
                                 FROM clean.sensor_readings WHERE is_known_mote)),
            grp AS (SELECT hr, hr - row_number() OVER (ORDER BY hr) * interval '1 hour' AS g FROM empty)
            SELECT min(hr), max(hr) + interval '1 hour', count(*) FROM grp GROUP BY g ORDER BY 1""")
        md += ["## 6. Network-wide outages", "",
               "Hours in which no registered mote reported at all (base station / logging outage, "
               "not individual mote failures). These hours also count as gaps in each mote's health metrics.", "",
               _table(["from", "to", "hours"], outages) if outages else "_None._", ""]

        # ---- 7. Motes with the most flagged readings -----------------------
        # The most common flag is shown because "flagged" mixes very different
        # problems: e.g. mote 28 is 100% flagged only because it never reports light.
        worst = _q(cur, """
            WITH m AS (
                SELECT moteid, sum(reading_count) AS n, sum(flagged_count) AS f
                FROM marts.mote_daily GROUP BY 1 HAVING sum(reading_count) > 0),
            top_flag AS (
                SELECT DISTINCT ON (moteid) moteid, flag, sum(readings) AS r
                FROM marts.anomaly_counts GROUP BY moteid, flag
                ORDER BY moteid, sum(readings) DESC)
            SELECT m.moteid, m.n, m.f, round(100.0 * m.f / m.n, 1), t.flag, t.r
            FROM m LEFT JOIN top_flag t USING (moteid)
            ORDER BY 4 DESC, 1 LIMIT 5""")
        md += ["## 7. Motes with the highest share of flagged readings", "",
               _table(["mote", "readings", "flagged", "flagged %", "most common flag", "rows with it"],
                      [[m, _n(n), _n(f), p, f"`{flag}`", _n(r)] for m, n, f, p, flag, r in worst]), ""]

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    # Airflow run ids contain ':' and '+', which are not valid in all file systems.
    path = REPORT_DIR / f"dq_report_{re.sub(r'[^A-Za-z0-9_.-]', '_', run_id)}.md"
    path.write_text("\n".join(md), encoding="utf-8")
    log.info("wrote %s", path)
    return path
