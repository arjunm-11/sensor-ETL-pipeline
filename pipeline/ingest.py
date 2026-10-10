"""
Raw ingestion: copy source lines into the raw layer, in fixed-size batches.

Batching model
--------------
The file is divided into a fixed grid of line ranges: with batch_size=500000,
batch 1 = lines 1..500000, batch 2 = 500001..1000000, and so on. The batch_id
is derived from the grid range ('data.txt.gz:000000001-000500000'), so the same
lines always land in the same batch, no matter when or how often we load.

Why line ranges and not time windows: the file is not time-ordered (227,448
lines are earlier than their predecessor), so "everything after time X" would
miss late-arriving lines.

Modes
-----
backfill     load every batch of the file. Lines already in raw are skipped
             (ON CONFLICT DO NOTHING), so a backfill over a loaded file is a
             no-op for raw, and every batch is handed to staging for rebuild.
incremental  start at the batch that contains the first line not yet loaded.
             Only batches that actually gained rows are returned. If the file
             grew since the last run, the previous partial batch is topped up
             and new batches are created.
"""
import gzip
import logging
from pathlib import Path

from . import db

log = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 500_000


def grid_bounds(line_no: int, batch_size: int) -> tuple[int, int]:
    """Grid range (1-based, inclusive) of the batch that contains line_no."""
    start = ((line_no - 1) // batch_size) * batch_size + 1
    return start, start + batch_size - 1


def make_batch_id(source_file: str, start: int, end: int) -> str:
    # Zero-padded so batch ids sort in file order as plain strings.
    return f"{source_file}:{start:09d}-{end:09d}"


def _open_lines(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    # latin-1 maps every byte to a character, so decoding can never fail or
    # alter content: raw keeps exactly what the file contained. The file
    # itself is plain ASCII. newline="" stops Python translating '\r\n'.
    return opener(path, "rt", encoding="latin-1", newline="")


def _iter_batches(path: Path, batch_size: int, first_line: int):
    """Yield (grid_start, grid_end, [(line_no, line), ...]) from first_line on."""
    current, rows = None, []
    with _open_lines(path) as fh:
        for line_no, line in enumerate(fh, start=1):
            if line_no < first_line:
                continue  # gzip can't seek by line, so earlier lines are read and skipped
            bounds = grid_bounds(line_no, batch_size)
            if bounds != current and rows:
                yield (*current, rows)
                rows = []
            current = bounds
            # Strip only the line terminator. Trailing spaces are data here:
            # they encode empty trailing fields (526 lines end with spaces).
            rows.append((line_no, line.rstrip("\n")))
    if rows:
        yield (*current, rows)


def _load_batch(source_file: str, start: int, end: int, rows: list) -> tuple[str, int]:
    """Load one batch in a single transaction. Returns (batch_id, newly inserted rows)."""
    batch_id = make_batch_id(source_file, start, end)
    last_line = rows[-1][0]
    with db.transaction() as cur:
        # The batch row must exist first (raw.sensor_readings has an FK to it).
        cur.execute(
            """INSERT INTO raw.load_batches (batch_id, source_file, line_start, line_end, rows_loaded)
               VALUES (%s, %s, %s, %s, 0)
               ON CONFLICT (batch_id) DO NOTHING""",
            (batch_id, source_file, start, last_line),
        )
        # COPY can't do ON CONFLICT, so COPY into a temp table, then insert
        # only the lines not already present. ON COMMIT DROP cleans it up.
        cur.execute("""CREATE TEMP TABLE tmp_raw (source_line_no BIGINT, raw_line TEXT) ON COMMIT DROP""")
        db.copy_text_rows(cur, "tmp_raw", ["source_line_no", "raw_line"], rows)
        cur.execute(
            """INSERT INTO raw.sensor_readings (source_file, source_line_no, batch_id, raw_line)
               SELECT %s, source_line_no, %s, raw_line FROM tmp_raw
               ON CONFLICT (source_file, source_line_no) DO NOTHING""",
            (source_file, batch_id),
        )
        inserted = cur.rowcount
        cur.execute(
            """UPDATE raw.load_batches
               SET line_end = GREATEST(line_end, %s),
                   rows_loaded = (SELECT count(*) FROM raw.sensor_readings WHERE batch_id = %s),
                   loaded_at = now()
               WHERE batch_id = %s""",
            (last_line, batch_id, batch_id),
        )
    return batch_id, inserted


def ingest_readings(path: Path, mode: str = "incremental", batch_size: int = DEFAULT_BATCH_SIZE) -> list[str]:
    """Load data.txt(.gz) into raw. Returns the batch_ids that downstream steps should (re)process."""
    if mode not in ("backfill", "incremental"):
        raise ValueError(f"unknown mode {mode!r}")
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Download data.txt.gz from http://db.csail.mit.edu/labdata/labdata.html "
            "into data/raw/ (see README).")
    source_file = path.name
    with db.transaction() as cur:
        # Batch ids encode a fixed line grid (D12). Loading the same file with a
        # different batch size would create overlapping batches, so refuse it.
        cur.execute("SELECT count(*) FROM raw.load_batches WHERE source_file = %s AND (line_start - 1) %% %s <> 0",
                    (source_file, batch_size))
        if cur.fetchone()[0]:
            raise ValueError(f"{source_file} was loaded with a different batch size than {batch_size}; "
                             "keep the original batch size (or reset the database).")
    first_line = 1
    if mode == "incremental":
        with db.transaction() as cur:
            cur.execute("SELECT max(line_end) FROM raw.load_batches WHERE source_file = %s", (source_file,))
            max_loaded = cur.fetchone()[0] or 0
        first_line = max_loaded + 1
        # Begin at the start of that line's grid batch, so the batch is complete
        # in memory; already-loaded lines are skipped by ON CONFLICT.
        first_line = grid_bounds(first_line, batch_size)[0]
        log.info("incremental: last loaded line %s, resuming at line %s", max_loaded, first_line)

    touched = []
    for start, end, rows in _iter_batches(path, batch_size, first_line):
        batch_id, inserted = _load_batch(source_file, start, end, rows)
        log.info("batch %s: %s lines read, %s newly inserted", batch_id, len(rows), inserted)
        if mode == "backfill" or inserted > 0:
            touched.append(batch_id)
    if not touched:
        log.info("no new lines in %s", source_file)
    return touched


def ingest_reference(path: Path, table: str) -> int:
    """Load a small reference file (mote_locs / connectivity) into raw.<table>, idempotently."""
    with _open_lines(path) as fh:
        rows = [(i, line.rstrip("\n")) for i, line in enumerate(fh, start=1)]
    with db.transaction() as cur:
        cur.execute("CREATE TEMP TABLE tmp_ref (source_line_no BIGINT, raw_line TEXT) ON COMMIT DROP")
        db.copy_text_rows(cur, "tmp_ref", ["source_line_no", "raw_line"], rows)
        cur.execute(
            f"""INSERT INTO raw.{table} (source_line_no, raw_line)
                SELECT source_line_no, raw_line FROM tmp_ref
                ON CONFLICT (source_line_no) DO NOTHING"""
        )
        inserted = cur.rowcount
    log.info("raw.%s: %s lines read, %s newly inserted", table, len(rows), inserted)
    return inserted
