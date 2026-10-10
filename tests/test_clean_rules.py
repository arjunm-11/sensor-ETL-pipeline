"""
Integration tests for the validation rules in sql/transform/clean.sql.

The rules are SQL, so they're tested against a real Postgres: a throwaway
database `wsn_test` is created from sql/init/, hand-made staging rows are
inserted (one scenario per rule), the real clean step runs, and the
resulting flags are asserted. Skipped when Postgres is not reachable.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import pytest

from pipeline import clean, db

TEST_DB = "wsn_test"
INIT_DIR = Path(__file__).resolve().parent.parent / "sql" / "init"
T0 = datetime(2004, 3, 1, 10, 0, 0)
NORMAL = dict(t=20.0, h=40.0, l=100.0, v=2.6)


def _admin_conn():
    conn = db.connect()           # default database (wsn)
    conn.autocommit = True        # CREATE/DROP DATABASE can't run inside a transaction
    return conn


def build_rows():
    """Return {line_no: (ts, epoch, moteid, t, h, l, v)} covering each rule."""
    rows, n = {}, iter(range(1, 1000))

    def add(ts, epoch, mote, t, h, l, v):
        line = next(n)
        rows[line] = (ts, epoch, mote, t, h, l, v)
        return line

    s = {}
    # Mote 1: steady 20 C every 30 s, with one isolated blip to 30 C at i=5.
    for i in range(10):
        s[("m1", i)] = add(T0 + timedelta(seconds=30 * i), 1000 + i, 1,
                           30.0 if i == 5 else 20.0, 40.0, 100.0, 2.6)
    # Mote 2: a genuine step change 20 -> 30 C at i=5 (must NOT be a spike).
    for i in range(10):
        s[("m2", i)] = add(T0 + timedelta(seconds=30 * i), 1000 + i, 2,
                           30.0 if i >= 5 else 20.0, 40.0, 100.0, 2.6)
    m2_2 = rows[s[("m2", 2)]]
    # Same packet received again 0.5 s later -> duplicate, removed.
    s["dup"] = add(m2_2[0] + timedelta(seconds=0.5), *m2_2[1:])
    # Same mote+epoch, different value, 1 s later -> NOT a duplicate (epoch wrap / reboot case).
    m2_3 = rows[s[("m2", 3)]]
    s["same_epoch_diff_values"] = add(m2_3[0] + timedelta(seconds=1), m2_3[1], 2, 20.5, 40.0, 100.0, 2.6)
    # Identical values but 61 s after the original -> outside the 60 s window, kept.
    m2_0 = rows[s[("m2", 0)]]
    s["same_values_61s"] = add(m2_0[0] + timedelta(seconds=61), *m2_0[1:])

    # Mote 3: one scenario per value rule.
    s["stuck"] = add(T0 + timedelta(minutes=2), 1004, 3, 122.153, -3.91901, 50.0, 2.2)
    s["low_batt_plausible"] = add(T0 + timedelta(minutes=3), 1006, 3, 21.0, 40.0, 50.0, 2.25)
    s["light_missing"] = add(T0 + timedelta(minutes=4), 1008, 3, 21.0, 40.0, None, 2.6)
    s["epoch_off_and_volt_glitch"] = add(T0 + timedelta(minutes=5), 5, 3, 21.0, 40.0, 50.0, 18.56)
    s["cold"] = add(T0 + timedelta(minutes=7), 1014, 3, -0.5, 40.0, 50.0, 2.6)
    # Unregistered mote id.
    s["unknown"] = add(T0 + timedelta(minutes=6), 1012, 58, **{"t": 20.0, "h": 40.0, "l": 100.0, "v": 2.6})

    # Hour 11: network epoch near the 16-bit wrap (65530..65535). A reading
    # with epoch 2 is 3-8 ticks from consensus on a circular counter -> not flagged.
    t11 = T0 + timedelta(hours=1)
    for i in range(6):
        add(t11 + timedelta(seconds=30 * i), 65530 + i, 1, 20.0, 40.0, 100.0, 2.6)
    s["after_wrap"] = add(t11 + timedelta(minutes=3), 2, 1, 20.0, 40.0, 100.0, 2.6)
    return rows, s


@pytest.fixture(scope="module")
def cleaned():
    try:
        admin = _admin_conn()
    except psycopg2.OperationalError:
        pytest.skip("Postgres not reachable (start it with `docker compose up -d postgres`)")
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    admin.close()

    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("PGDATABASE", TEST_DB)       # db.connect() reads it on every call
        with db.transaction() as cur:
            # 00_* creates the Airflow metadata database: not part of the warehouse schema.
            for f in sorted(INIT_DIR.glob("*.sql")):
                if not f.name.startswith("00_"):
                    cur.execute(f.read_text(encoding="utf-8"))
            cur.execute("INSERT INTO staging.mote_locs VALUES (1, 0, 0), (2, 1, 1), (3, 2, 2)")
            rows, scenarios = build_rows()
            for line, (ts, epoch, mote, t, h, l, v) in rows.items():
                cur.execute(
                    """INSERT INTO staging.sensor_readings
                       (source_file, source_line_no, batch_id, reading_ts, epoch, moteid,
                        temperature, humidity, light, voltage)
                       VALUES ('test', %s, 'b1', %s, %s, %s, %s, %s, %s, %s)""",
                    (line, ts, epoch, mote, t, h, l, v))

        results = {r.check_name: r for r in clean.build_clean("pytest")}
        with db.transaction() as cur:
            cur.execute("""SELECT source_line_no, dq_flags, temperature_ok, humidity_ok, light_ok, voltage_ok
                           FROM clean.sensor_readings""")
            out = {r[0]: {"flags": set(r[1]), "ok": r[2:]} for r in cur.fetchall()}
        yield out, scenarios, results, len(rows)

    admin = _admin_conn()
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
    admin.close()


def flags(cleaned, key):
    out, s, *_ = cleaned
    return out[s[key]]["flags"]


def ok(cleaned, key):
    """(temperature_ok, humidity_ok, light_ok, voltage_ok)"""
    out, s, *_ = cleaned
    return out[s[key]]["ok"]


# ---- deduplication ----------------------------------------------------------

def test_same_packet_twice_within_60s_is_removed(cleaned):
    out, s, results, n = cleaned
    assert s["dup"] not in out
    assert s[("m2", 2)] in out                       # the first copy is kept
    assert len(out) == n - 1
    assert results["duplicates_removed"].rows_failed == 1


def test_same_mote_and_epoch_with_different_values_is_not_a_duplicate(cleaned):
    out, s, *_ = cleaned
    assert s["same_epoch_diff_values"] in out


def test_identical_values_more_than_60s_apart_are_kept(cleaned):
    out, s, *_ = cleaned
    assert s["same_values_61s"] in out


def test_post_conditions_pass(cleaned):
    _, _, results, _ = cleaned
    assert results["no_rows_lost"].passed
    assert results["no_duplicates_remaining"].passed


# ---- value rules ------------------------------------------------------------

def test_normal_reading_has_no_flags(cleaned):
    assert flags(cleaned, ("m1", 0)) == set()
    assert ok(cleaned, ("m1", 0)) == (True, True, True, True)


def test_stuck_values_at_low_voltage(cleaned):
    assert flags(cleaned, "stuck") == {"temperature_out_of_range", "humidity_out_of_range", "low_battery"}
    # Light is unaffected by these rules and stays usable.
    assert ok(cleaned, "stuck") == (False, False, True, True)


def test_low_battery_invalidates_plausible_temperature_and_humidity(cleaned):
    assert flags(cleaned, "low_batt_plausible") == {"low_battery"}
    assert ok(cleaned, "low_batt_plausible") == (False, False, True, True)


def test_below_zero_is_out_of_range(cleaned):
    assert flags(cleaned, "cold") == {"temperature_out_of_range"}


def test_missing_value_is_flagged_and_not_usable(cleaned):
    assert flags(cleaned, "light_missing") == {"light_missing"}
    assert ok(cleaned, "light_missing") == (True, True, False, True)


def test_voltage_glitch_and_epoch_far_from_consensus(cleaned):
    assert flags(cleaned, "epoch_off_and_volt_glitch") == {"voltage_out_of_range", "epoch_inconsistent"}
    # epoch_inconsistent is informational; 18.56 V is not "low battery".
    assert ok(cleaned, "epoch_off_and_volt_glitch") == (True, True, True, False)


def test_unregistered_mote_is_flagged_not_dropped(cleaned):
    assert flags(cleaned, "unknown") == {"unknown_mote"}


# ---- contextual rules -------------------------------------------------------

def test_isolated_blip_is_a_spike(cleaned):
    assert flags(cleaned, ("m1", 5)) == {"temperature_spike"}
    assert ok(cleaned, ("m1", 5))[0] is False
    # Its neighbours differ from it but not from each other: not spikes.
    assert flags(cleaned, ("m1", 4)) == set() and flags(cleaned, ("m1", 6)) == set()


def test_step_change_is_not_a_spike(cleaned):
    assert "temperature_spike" not in flags(cleaned, ("m2", 5))
    assert "temperature_spike" not in flags(cleaned, ("m2", 4))


def test_epoch_check_is_wrap_aware(cleaned):
    assert "epoch_inconsistent" not in flags(cleaned, "after_wrap")
