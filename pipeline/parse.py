"""
Parsing raw lines of data.txt into typed staging rows.

Pure functions only (no database access), so they can be unit-tested directly.

Line format (see docs/findings.md #1):
    date time epoch moteid temperature humidity light voltage
separated by exactly ONE space. An empty field shows up as two consecutive
spaces, so a valid line always splits into exactly 8 tokens with str.split(" ").
"""
import pandas as pd

FIELDS = ["date", "time", "epoch", "moteid", "temperature", "humidity", "light", "voltage"]
SENSOR_COLS = ["temperature", "humidity", "light", "voltage"]

# Strict token shapes. We validate shape with a regex *before* converting,
# because pandas' converters are lenient: to_numeric accepts "nan"/"inf", and
# ISO8601 datetime parsing accepts forms that never appear in this file.
DATE_RE = r"^\d{4}-\d{2}-\d{2}$"
TIME_RE = r"^\d{2}:\d{2}:\d{2}(\.\d{1,6})?$"   # 4 lines in the file have no fraction
UINT_RE = r"^\d+$"
FLOAT_RE = r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$"

INT32_MAX = 2**31 - 1  # epoch/moteid are stored as INTEGER in Postgres


def split_lines(lines: pd.Series) -> pd.DataFrame:
    """
    Split raw lines into the 8 named fields.

    Lines that don't split into exactly 8 tokens get all fields as NA and
    `field_count` records how many they had (used for the reject reason).
    """
    # Keep the separator as a single literal space: n=-1 means split on every
    # space, regex=False so " " is not interpreted as a pattern.
    parts = lines.str.split(" ", n=-1, regex=False)
    field_count = parts.str.len()
    ok = field_count == len(FIELDS)
    out = pd.DataFrame(index=lines.index, columns=FIELDS, dtype="object")
    if ok.any():
        out.loc[ok, FIELDS] = pd.DataFrame(parts[ok].tolist(), index=lines.index[ok], columns=FIELDS)
    out["field_count"] = field_count
    return out


def parse_lines(lines: pd.Series) -> tuple[pd.DataFrame, pd.Series]:
    """
    Parse raw lines into typed columns.

    Returns:
        typed   DataFrame with reading_ts, epoch, moteid, temperature,
                humidity, light, voltage (same index as `lines`).
        reasons Series of reject reasons ('' = row is OK). Multiple problems
                are joined with ';' so nothing is hidden behind the first error.

    Reject policy (agreed in Phase 1): a row is rejected only if it can't be
    attributed to a mote at a point in time, or if it contains a token that is
    present but malformed. A row missing just some sensor values (e.g. light)
    is kept with NULLs; plausibility of values is judged in the clean layer.
    """
    f = split_lines(lines)
    reasons = pd.Series("", index=lines.index, dtype="object")

    def add(mask: pd.Series, reason: str) -> None:
        mask = mask.fillna(False).astype(bool)
        reasons[mask] = reasons[mask].where(reasons[mask] == "", reasons[mask] + ";") + reason

    add(f["field_count"] != len(FIELDS), "wrong_field_count")
    has8 = f["field_count"] == len(FIELDS)

    # --- timestamp ---------------------------------------------------------
    d, t = f["date"].fillna(""), f["time"].fillna("")
    shape_ok = d.str.match(DATE_RE) & t.str.match(TIME_RE)
    ts = pd.Series(pd.NaT, index=lines.index, dtype="datetime64[ns]")
    if shape_ok.any():
        # Shape is already strictly validated; ISO8601 handles the optional
        # fraction. errors="coerce" turns impossible dates (2004-13-45) into NaT.
        ts[shape_ok] = pd.to_datetime(d[shape_ok] + " " + t[shape_ok], format="ISO8601", errors="coerce")
    add(has8 & ((d == "") | (t == "")), "missing_timestamp")
    add(has8 & (d != "") & (t != "") & ts.isna(), "invalid_timestamp")

    # --- integer key columns (epoch, moteid) ---------------------------------
    ints = {}
    for col in ["epoch", "moteid"]:
        tok = f[col].fillna("")
        valid = tok.str.match(UINT_RE)
        val = pd.to_numeric(tok.where(valid), errors="coerce")
        too_big = val > INT32_MAX
        add(has8 & (tok == ""), f"missing_{col}")
        add(has8 & (tok != "") & ~valid, f"invalid_{col}")
        add(too_big, f"{col}_out_of_int_range")
        ints[col] = val.where(~too_big)

    # --- sensor values: empty is allowed (NULL), malformed is not ------------
    sens = {}
    for col in SENSOR_COLS:
        tok = f[col].fillna("")
        valid = tok.str.match(FLOAT_RE)
        add(has8 & (tok != "") & ~valid, f"invalid_{col}")
        sens[col] = pd.to_numeric(tok.where(valid), errors="coerce")

    typed = pd.DataFrame({
        "reading_ts": ts,
        "epoch": ints["epoch"].astype("Int64"),
        "moteid": ints["moteid"].astype("Int64"),
        **{c: sens[c].astype("float64") for c in SENSOR_COLS},
    }, index=lines.index)
    return typed, reasons


def parse_mote_locs(lines: list[str]) -> pd.DataFrame:
    """mote_locs.txt: 'moteid x y' per line, whitespace-separated, no gaps."""
    rows = [l.split() for l in lines if l.strip()]
    df = pd.DataFrame(rows, columns=["moteid", "x", "y"])
    return df.astype({"moteid": "int64", "x": "float64", "y": "float64"})
