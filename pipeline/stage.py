"""
Staging: parse raw lines of a batch into typed rows; unparseable rows go to
staging.rejects with a reason.

Idempotency: staging is fully derived from raw, so each batch is rebuilt by
DELETE-then-INSERT inside one transaction. Rerunning a batch replaces its
rows instead of adding to them, and a failure mid-way rolls back to the
previous state.
"""
import logging

import pandas as pd

from . import db
from .parse import parse_lines, parse_mote_locs

log = logging.getLogger(__name__)

STAGED_COLS = ["source_file", "source_line_no", "batch_id", "reading_ts", "epoch", "moteid",
               "temperature", "humidity", "light", "voltage"]


def stage_batch(batch_id: str) -> dict:
    with db.transaction() as cur:
        cur.execute(
            "SELECT source_file, source_line_no, raw_line FROM raw.sensor_readings WHERE batch_id = %s",
            (batch_id,),
        )
        raw = pd.DataFrame(cur.fetchall(), columns=["source_file", "source_line_no", "raw_line"])
        if raw.empty:
            raise ValueError(f"batch {batch_id} has no raw rows; was it ingested?")

        typed, reasons = parse_lines(raw["raw_line"])
        rejected = reasons != ""

        staged = pd.concat([raw[["source_file", "source_line_no"]], typed], axis=1)[~rejected]
        staged.insert(2, "batch_id", batch_id)
        rejects = raw.loc[rejected, ["source_file", "source_line_no"]].assign(
            batch_id=batch_id, reject_reason=reasons[rejected])

        cur.execute("DELETE FROM staging.sensor_readings WHERE batch_id = %s", (batch_id,))
        cur.execute("DELETE FROM staging.rejects WHERE batch_id = %s", (batch_id,))
        db.copy_dataframe(cur, "staging.sensor_readings", staged[STAGED_COLS])
        db.copy_dataframe(cur, "staging.rejects", rejects)

    stats = {"batch_id": batch_id, "raw": len(raw), "staged": len(staged), "rejected": len(rejects)}
    # Every raw row must end up in exactly one of the two tables.
    assert stats["staged"] + stats["rejected"] == stats["raw"], stats
    log.info("staged %s", stats)
    if len(rejects):
        log.info("reject reasons in %s: %s", batch_id, reasons[rejected].value_counts().to_dict())
    return stats


def stage_mote_locs() -> int:
    with db.transaction() as cur:
        cur.execute("SELECT raw_line FROM raw.mote_locs ORDER BY source_line_no")
        locs = parse_mote_locs([r[0] for r in cur.fetchall()])
        # Small reference table: a full rebuild is the simplest idempotent option.
        cur.execute("TRUNCATE staging.mote_locs")
        db.copy_dataframe(cur, "staging.mote_locs", locs)
    log.info("staging.mote_locs: %s motes", len(locs))
    return len(locs)


def all_batch_ids() -> list[str]:
    with db.transaction() as cur:
        cur.execute("SELECT batch_id FROM raw.load_batches ORDER BY batch_id")
        return [r[0] for r in cur.fetchall()]
