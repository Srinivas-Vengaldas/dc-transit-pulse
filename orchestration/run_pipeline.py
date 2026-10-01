"""One step of the scheduled pipeline, run as a Databricks job task.

The job (orchestration/workflow.json) runs these tasks, each calling this file
with a different --step:

    collect --> bronze --> silver --> gold --> quality
    weather   (independent: a weather API outage never blocks the transit tables)

- collect: polls WMATA for --polls snapshots and writes them straight into the
  landing folder of the raw Volume (the API key comes from a Databricks secret).
- bronze / silver / gold: the existing pipeline functions.
- quality: data-quality checks on the dates gold just rebuilt; fails the run on a critical failure.

Every step appends one row to ops_run_log (run id, step, start, end, status,
metrics as JSON), even when it fails. That table is where the resume metrics
come from: events per day (bronze rows added) and freshness (gold step).

Run by hand in a notebook-free way:
    python orchestration/run_pipeline.py --step gold
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

try:
    REPO_ROOT = Path(__file__).resolve().parents[1]
except NameError:  # some Databricks runners exec the file without __file__
    REPO_ROOT = Path.cwd()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

log = logging.getLogger("run_pipeline")

STEPS = ("collect", "bronze", "silver", "gold", "quality", "weather")
FEED_URLS = {
    "vehicle_positions": "https://api.wmata.com/gtfs/bus-gtfsrt-vehiclepositions.pb",
    "trip_updates": "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb",
}
RUN_LOG_SCHEMA = (
    "run_id string, step string, started_at timestamp, finished_at timestamp, status string, metrics string"
)


def wmata_api_key(scope: str, key: str) -> str:
    """Read the WMATA key from a Databricks secret scope (never from code or job parameters)."""
    from databricks.sdk.runtime import dbutils

    return dbutils.secrets.get(scope=scope, key=key)


def collect(raw_volume: str, polls: int, api_key: str, interval_s: int = 30) -> dict[str, int]:
    """Poll WMATA `polls` times and land the snapshots in the raw Volume's landing folder."""
    import requests

    from producer.fetch_gtfs_rt import Config, poll_once

    cfg = Config(api_key=api_key, feeds=FEED_URLS, landing_dir=Path(raw_volume) / "landing",
                 poll_interval_s=interval_s)
    totals = dict.fromkeys(FEED_URLS, 0)
    with requests.Session() as session:
        for i in range(polls):
            started = time.monotonic()
            for feed, n in poll_once(session, cfg).items():
                totals[feed] += n
            if i < polls - 1:
                time.sleep(max(0.0, interval_s - (time.monotonic() - started)))
    if not any(totals.values()):
        raise RuntimeError(f"collect landed no records in {polls} polls")
    return {f"{feed}_records": n for feed, n in totals.items()}


def run_step(spark: SparkSession, step: str, args: argparse.Namespace) -> dict:
    """Run one step and return its metrics."""
    if step == "collect":
        return collect(args.raw_volume, args.polls, wmata_api_key(args.secret_scope, args.secret_key))
    if step == "bronze":
        from pipelines.bronze_ingest import ingest_all

        return {f"{feed}_rows_added": n for feed, n in ingest_all(spark, args.raw_volume).items()}
    if step == "silver":
        from pipelines.silver_transform import run_silver

        return run_silver(spark, args.raw_volume)
    if step == "gold":
        from pipelines.gold_marts import build_gold, gold_freshness_s

        out = build_gold(spark)
        return {"start": str(out["start"]), "end": str(out["end"]), "rows": out["rows"],
                "freshness_s": gold_freshness_s(spark)}
    if step == "quality":
        from pipelines.quality import run_checks

        return run_checks(spark, run_id=args.run_id)
    if step == "weather":
        from pipelines.weather import load_weather

        return load_weather(spark)
    raise ValueError(f"unknown step {step!r}; expected one of {STEPS}")


def log_run(spark: SparkSession, table: str, run_id: str, step: str, started: datetime, status: str,
            metrics: dict) -> None:
    """Append one row to the run log."""
    row = (run_id, step, started, datetime.now(UTC), status, json.dumps(metrics, default=str))
    spark.createDataFrame([row], RUN_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(table)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--step", choices=STEPS, required=True)
    p.add_argument("--run-id", default="manual")
    p.add_argument("--polls", type=int, default=20, help="collect: snapshots to take, 30 s apart")
    p.add_argument("--raw-volume", default="/Volumes/workspace/transit/raw")
    p.add_argument("--run-log", default="workspace.transit.ops_run_log")
    p.add_argument("--secret-scope", default="transit")
    p.add_argument("--secret-key", default="wmata_api_key")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the job task. A failure is logged, then re-raised so the job retries the task."""
    from pyspark.sql import SparkSession

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    started = datetime.now(UTC)
    try:
        metrics = run_step(spark, args.step, args)
    except Exception as exc:
        log_run(spark, args.run_log, args.run_id, args.step, started, "failed",
                {"error": f"{type(exc).__name__}: {exc}"[:2000]})
        raise
    log_run(spark, args.run_log, args.run_id, args.step, started, "succeeded", metrics)
    log.info("%s succeeded: %s", args.step, metrics)
    return 0


if __name__ == "__main__":
    # No sys.exit(): on Databricks a SystemExit can be reported as a task failure. Errors still raise.
    main()
