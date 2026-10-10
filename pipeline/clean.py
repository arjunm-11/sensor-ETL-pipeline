"""Build the clean layer: dedup + flags (sql/transform/clean.sql), then verify it."""
import logging
from pathlib import Path

from . import db, quality

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def build_clean(run_id: str) -> list[quality.CheckResult]:
    sql = (SQL_DIR / "transform" / "clean.sql").read_text(encoding="utf-8")
    results = []
    try:
        # Rebuild and post-condition checks share ONE transaction: if an
        # error-severity check fails we raise inside it, Postgres rolls back the
        # TRUNCATE+INSERT, and clean keeps its last good contents.
        with db.transaction() as cur:
            cur.execute(sql)
            log.info("clean.sensor_readings rebuilt: %s rows", cur.rowcount)
            results = quality.clean_checks(cur)
            quality.raise_on_errors(results)
    finally:
        # Record in a separate transaction so results survive a rollback.
        if results:
            quality.record(run_id, results)
    return results
