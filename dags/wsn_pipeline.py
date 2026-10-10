"""
### Intel Berkeley Lab sensor pipeline

`ingest → stage → validate → clean → build_marts → quality_report`

* **ingest**: new lines of `data.txt.gz` into `raw` in fixed 500K-line batches (idempotent).
* **stage**: parse the batches ingest touched into `staging`; bad lines go to `staging.rejects`.
* **validate**: gate checks on staging (reconciliation, reject rate, mote registry).
* **clean**: dedup + data-quality flags, rebuilt in full with post-condition checks.
* **build_marts**: daily per-mote aggregates, mote health, anomaly counts.
* **quality_report**: `reports/dq_report_<run_id>.md`.

Trigger with config `{"mode": "backfill"}` to reprocess every batch; the default
`incremental` loads only lines not yet in raw. All logic lives in the `pipeline`
package; this file only wires it together.
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

log = logging.getLogger(__name__)


def log_failure(context) -> None:
    """on_failure_callback: one clear line in the task log once retries are exhausted.
    Alerting (email/Slack) would hook in here; not configured for this project."""
    ti = context["task_instance"]
    log.error("TASK FAILED dag=%s task=%s run=%s try=%s error=%r",
              ti.dag_id, ti.task_id, context["run_id"], ti.try_number, context.get("exception"))


def no_retry_on_dq_failure(fn, *args):
    """A failed data-quality check is deterministic: retrying gives the same
    result. AirflowFailException fails the task immediately, skipping the
    retries meant for transient problems (database restart, network blip)."""
    from pipeline.quality import DataQualityError
    try:
        return fn(*args)
    except DataQualityError as e:
        raise AirflowFailException(f"data-quality check failed: {e}") from e


@dag(
    dag_id="wsn_sensor_pipeline",
    description="Intel Berkeley Lab sensor ETL: raw -> staging -> clean -> marts",
    doc_md=__doc__,
    # Daily incremental run: picks up lines appended to the file since the last
    # run (none for this static dataset, which makes the run cheap and safe).
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,           # don't create a run for every day since start_date
    max_active_runs=1,       # runs rebuild shared tables; never let two overlap
    params={"mode": Param("incremental", enum=["incremental", "backfill"],
                          description="incremental: only new lines; backfill: reprocess every batch")},
    default_args={
        "owner": "wsn",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,          # 30 s, then ~60 s
        "execution_timeout": timedelta(minutes=30),  # a hung task fails instead of blocking forever
        "on_failure_callback": log_failure,
    },
    tags=["wsn", "etl"],
)
def wsn_sensor_pipeline():
    # Imports inside tasks keep DAG parsing fast (the scheduler re-parses this
    # file every 30 s) and keep pandas out of the scheduler's parse loop.

    @task
    def ingest(params=None) -> list[str]:
        """Returns the batch ids downstream tasks must (re)process, via XCom."""
        import os
        from pipeline import ingest as ing
        data_dir = Path(os.environ["WSN_DATA_DIR"])
        ing.ingest_reference(data_dir / "mote_locs.txt", "mote_locs")
        ing.ingest_reference(data_dir / "connectivity.txt", "connectivity")
        return ing.ingest_readings(data_dir / "data.txt.gz", mode=params["mode"])

    @task
    def stage(batch_ids: list[str]) -> list[str]:
        from pipeline import stage as stg
        stg.stage_mote_locs()
        for b in batch_ids:
            stg.stage_batch(b)
        log.info("staged %d batch(es)", len(batch_ids))
        return batch_ids

    @task
    def validate(batch_ids: list[str], run_id=None) -> None:
        from pipeline import quality
        no_retry_on_dq_failure(quality.validate_staging, batch_ids, run_id)

    @task
    def clean(run_id=None) -> None:
        from pipeline import clean as cln
        no_retry_on_dq_failure(cln.build_clean, run_id)

    @task
    def build_marts(run_id=None) -> None:
        from pipeline import marts
        no_retry_on_dq_failure(marts.build_marts, run_id)

    @task
    def quality_report(run_id=None) -> str:
        from pipeline import report
        return str(report.build_report(run_id))

    # XCom passes batch ids ingest → stage → validate; the rest are ordered with >>.
    validated = validate(stage(ingest()))
    validated >> clean() >> build_marts() >> quality_report()


wsn_sensor_pipeline()
