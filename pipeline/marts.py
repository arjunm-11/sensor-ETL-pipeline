"""Build the marts (sql/transform/marts.sql) and reconcile them against clean."""
import logging

from . import db, quality
from .clean import SQL_DIR

log = logging.getLogger(__name__)


def build_marts(run_id: str) -> list[quality.CheckResult]:
    sql = (SQL_DIR / "transform" / "marts.sql").read_text(encoding="utf-8")
    results = []
    try:
        # Same pattern as clean: rebuild + checks in one transaction, so a
        # failed reconciliation rolls back to the previous marts.
        with db.transaction() as cur:
            cur.execute(sql)
            results = quality.marts_checks(cur)
            quality.raise_on_errors(results)
    finally:
        if results:
            quality.record(run_id, results)
    return results
