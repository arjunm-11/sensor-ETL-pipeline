"""
Command-line entry point, so each step can be run by hand outside Airflow:

    python -m pipeline ingest --mode backfill
    python -m pipeline ingest --mode incremental
    python -m pipeline stage                 # all batches
    python -m pipeline stage --batch-id 'data.txt.gz:000000001-000500000'
    python -m pipeline validate              # staging gate checks -> audit.dq_results
    python -m pipeline clean                 # rebuild clean layer + its checks
    python -m pipeline marts                 # rebuild marts + reconciliation checks
    python -m pipeline report --run-id X     # markdown report for run X -> reports/
"""
import argparse
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from . import clean, ingest, marts, quality, report, stage

DATA_DIR = Path(os.environ.get("WSN_DATA_DIR", "data/raw"))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    pi = sub.add_parser("ingest")
    pi.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    pi.add_argument("--batch-size", type=int, default=ingest.DEFAULT_BATCH_SIZE)
    ps = sub.add_parser("stage")
    ps.add_argument("--batch-id", action="append", help="repeatable; default = all loaded batches")
    pv = sub.add_parser("validate")
    pv.add_argument("--batch-id", action="append", help="repeatable; default = all loaded batches")
    pc = sub.add_parser("clean")
    pm = sub.add_parser("marts")
    pr = sub.add_parser("report")
    for sp in (pv, pc, pm, pr):
        sp.add_argument("--run-id", default=f"manual__{datetime.now():%Y%m%dT%H%M%S}")
    args = p.parse_args()

    t0 = time.perf_counter()
    if args.cmd == "ingest":
        ingest.ingest_reference(DATA_DIR / "mote_locs.txt", "mote_locs")
        ingest.ingest_reference(DATA_DIR / "connectivity.txt", "connectivity")
        batches = ingest.ingest_readings(DATA_DIR / "data.txt.gz", args.mode, args.batch_size)
        logging.info("batches to process downstream: %s", batches)
    elif args.cmd == "stage":
        stage.stage_mote_locs()
        for b in args.batch_id or stage.all_batch_ids():
            stage.stage_batch(b)
    elif args.cmd == "validate":
        quality.validate_staging(args.batch_id or stage.all_batch_ids(), args.run_id)
    elif args.cmd == "clean":
        clean.build_clean(args.run_id)
    elif args.cmd == "marts":
        marts.build_marts(args.run_id)
    elif args.cmd == "report":
        report.build_report(args.run_id)
    logging.info("%s finished in %.1fs", args.cmd, time.perf_counter() - t0)


if __name__ == "__main__":
    main()
