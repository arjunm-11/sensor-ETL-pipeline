"""Unit tests for pipeline/parse.py (no database needed)."""
import pandas as pd
import pytest

from pipeline.parse import parse_lines, parse_mote_locs, split_lines

GOOD = "2004-02-28 00:59:16.02785 3 1 19.9884 37.0933 45.08 2.69964"


def parse_one(line: str):
    typed, reasons = parse_lines(pd.Series([line]))
    return typed.iloc[0], reasons.iloc[0]


# ---- splitting --------------------------------------------------------------

def test_complete_line_parses_to_typed_values():
    row, reason = parse_one(GOOD)
    assert reason == ""
    assert row.reading_ts == pd.Timestamp("2004-02-28 00:59:16.027850")
    assert (row.epoch, row.moteid) == (3, 1)
    assert row.temperature == pytest.approx(19.9884)
    assert row.voltage == pytest.approx(2.69964)


def test_double_space_is_an_empty_field_not_a_separator_run():
    # Real line from the file: light is empty. A whitespace split would put
    # 2.54901 (voltage) into the light column.
    row, reason = parse_one("2004-03-18 18:11:17.68583 56786 28 21.7818 44.9121  2.54901")
    assert reason == ""
    assert pd.isna(row.light)
    assert row.voltage == pytest.approx(2.54901)


def test_trailing_spaces_encode_empty_trailing_fields():
    # Real line: only date, time, epoch present; 5 trailing spaces -> 8 fields.
    f = split_lines(pd.Series(["2004-02-28 00:58:15.315133 1     "]))
    assert f.loc[0, "field_count"] == 8
    assert f.loc[0, "moteid"] == "" and f.loc[0, "voltage"] == ""


@pytest.mark.parametrize("line", ["garbage", GOOD + " 1", GOOD.rsplit(" ", 1)[0]])
def test_wrong_number_of_fields_is_rejected(line):
    row, reason = parse_one(line)
    assert reason == "wrong_field_count"
    assert pd.isna(row.reading_ts)


# ---- reject rules -----------------------------------------------------------

def test_missing_moteid_is_rejected():
    _, reason = parse_one("2004-02-28 00:58:15.315133 1     ")
    assert reason == "missing_moteid"


def test_missing_sensor_values_are_kept_as_null_not_rejected():
    row, reason = parse_one("2004-03-01 12:00:00.5 5 2   45.0 2.6")
    assert reason == ""
    assert pd.isna(row.temperature) and pd.isna(row.humidity)
    assert row.light == 45.0


def test_timestamp_without_fraction_is_accepted():
    # 4 lines in the real file have no fractional seconds.
    row, reason = parse_one("2004-03-01 12:00:00 5 2 20 40 45 2.6")
    assert reason == ""
    assert row.reading_ts == pd.Timestamp("2004-03-01 12:00:00")


@pytest.mark.parametrize("date,time", [("2004-13-01", "12:00:00"), ("2004-02-30", "12:00:00"),
                                       ("2004/03/01", "12:00:00"), ("2004-03-01", "25:00:00"),
                                       ("2004-03-01", "12:00")])
def test_invalid_timestamps_are_rejected(date, time):
    _, reason = parse_one(f"{date} {time} 5 2 20 40 45 2.6")
    assert reason == "invalid_timestamp"


@pytest.mark.parametrize("token", ["nan", "inf", "-inf", "abc", "1.2.3", "--4"])
def test_malformed_sensor_tokens_are_rejected_not_silently_nan(token):
    # pd.to_numeric would accept "nan"/"inf"; the strict regex must not.
    _, reason = parse_one(f"2004-03-01 12:00:00 5 2 {token} 40 45 2.6")
    assert reason == "invalid_temperature"


@pytest.mark.parametrize("token,ok", [("-3.91901", True), ("122.153", True), ("1e3", True),
                                      (".5", True), ("+2", True), ("-4", True)])
def test_valid_numeric_shapes(token, ok):
    _, reason = parse_one(f"2004-03-01 12:00:00 5 2 {token} 40 45 2.6")
    assert (reason == "") is ok


@pytest.mark.parametrize("epoch,mote,expected", [
    ("-1", "2", "invalid_epoch"),          # negative: not an unsigned counter
    ("1.5", "2", "invalid_epoch"),
    ("5", "x", "invalid_moteid"),
    ("5", "99999999999", "moteid_out_of_int_range"),
])
def test_key_column_rules(epoch, mote, expected):
    _, reason = parse_one(f"2004-03-01 12:00:00 {epoch} {mote} 20 40 45 2.6")
    assert reason == expected


def test_all_reasons_are_reported_not_just_the_first():
    _, reason = parse_one("2004-13-01 12:00:00 x  nan 40 45 2.6")
    assert reason.split(";") == ["invalid_timestamp", "invalid_epoch", "missing_moteid", "invalid_temperature"]


def test_unknown_but_well_formed_mote_id_is_not_a_reject():
    # Plausibility (is mote 6485 registered?) is a clean-layer flag, not a parse error.
    _, reason = parse_one("2004-03-01 12:00:00 5 6485 20 40 45 2.6")
    assert reason == ""


def test_batch_keeps_index_alignment():
    lines = pd.Series([GOOD, "garbage", GOOD], index=[10, 11, 12])
    typed, reasons = parse_lines(lines)
    assert list(typed.index) == [10, 11, 12]
    assert reasons.tolist() == ["", "wrong_field_count", ""]


# ---- reference file ---------------------------------------------------------

def test_parse_mote_locs_skips_blank_lines():
    df = parse_mote_locs(["1 21.5 23", "", "2 24.5 20"])
    assert df.moteid.tolist() == [1, 2]
    assert df.x.tolist() == [21.5, 24.5]
