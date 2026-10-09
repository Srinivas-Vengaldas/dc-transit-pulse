"""Pipeline metrics from the run log, data-quality results and table counts.

Every number that goes in the README or on a resume comes from one of the
queries below, so each can be re-run and checked:

    events per day      ops_run_log, bronze step: rows added per Eastern calendar day
    freshness           ops_run_log, gold step: now - newest observed departure, at build time
    rows removed        bronze rows vs silver + quarantine rows, per feed (duplicates + late rows)
    data quality        dq_results: share of checks passed, and runs where every check passed
    step runtime        ops_run_log: finished_at - started_at per step
    run coverage        ops_run_log: scheduled runs per day vs the 9 the schedule starts, and failures

Every query takes a measurement window of Eastern calendar days (inclusive). The
schedule runs 06:00-22:00 ET, so no run crosses midnight and a day is a clean unit.
Without a window the queries cover the whole log.

The queries are plain SQL, so the same text can be pasted into a Databricks SQL
cell; `metric_sql()` prints them with the names and window filled in, and
benchmarks/queries.sql is generated from them (`python -m pipelines.metrics`).

Usage in a Databricks notebook:
    from pipelines.metrics import MEASUREMENT_WINDOW, compute_metrics, save_metrics
    m = compute_metrics(spark, *MEASUREMENT_WINDOW)
    save_metrics(spark, m)
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("metrics")

LOCAL_TZ = "America/New_York"

# The fixed window the README and resume numbers come from: 7 full days of scheduled runs.
# Set before the data was in, so the numbers cannot be picked after the fact.
MEASUREMENT_WINDOW: tuple[str, str] = ("2026-10-02", "2026-10-08")

# The job starts at 06, 08, ..., 22 Eastern: 9 scheduled runs a day.
SCHEDULED_HOURS: tuple[int, ...] = tuple(range(6, 23, 2))
RUNS_PER_DAY = len(SCHEDULED_HOURS)

ALL_TIME: tuple[str, str] = ("1900-01-01", "9999-12-31")

# An Eastern calendar day of a timestamp column, inside the window.
IN_WINDOW = "to_date(from_utc_timestamp({col}, '{tz}')) BETWEEN DATE'{start}' AND DATE'{end}'"

METRIC_QUERIES: dict[str, str] = {
    # Rows the bronze step added, per Eastern calendar day. A retried attempt that
    # failed added nothing it did not later report, so only succeeded rows count.
    "events_per_day": """
SELECT to_date(from_utc_timestamp(started_at, '{tz}')) AS run_date,
       count(DISTINCT run_id) AS runs,
       sum(coalesce(cast(get_json_object(metrics, '$.vehicle_positions_rows_added') AS BIGINT), 0))
         AS vehicle_positions,
       sum(coalesce(cast(get_json_object(metrics, '$.trip_updates_rows_added') AS BIGINT), 0))
         AS trip_updates,
       sum(coalesce(cast(get_json_object(metrics, '$.vehicle_positions_rows_added') AS BIGINT), 0)
         + coalesce(cast(get_json_object(metrics, '$.trip_updates_rows_added') AS BIGINT), 0)) AS events
FROM {c}.{s}.ops_run_log
WHERE step = 'bronze' AND status = 'succeeded' AND {started_in_window}
GROUP BY 1
ORDER BY 1""",
    # Seconds from the newest observed departure to the moment gold finished building it.
    # Scheduled runs only: a manual rebuild hours after the last collect is not a freshness sample.
    "freshness": """
SELECT count(*) AS builds,
       percentile(freshness_s, 0.5) AS median_s,
       percentile(freshness_s, 0.95) AS p95_s,
       round(avg(freshness_s), 1) AS mean_s,
       min(freshness_s) AS min_s,
       max(freshness_s) AS max_s
FROM (
  SELECT cast(get_json_object(metrics, '$.freshness_s') AS INT) AS freshness_s
  FROM {c}.{s}.ops_run_log
  WHERE step = 'gold' AND status = 'succeeded' AND run_id <> 'manual' AND {started_in_window}
)
WHERE freshness_s IS NOT NULL""",
    # Table counts, not run-log sums: a replay or a retried step cannot skew them. Bronze and
    # silver rows are picked by the time bronze ingested them (silver keeps _ingested_at), and
    # quarantined rows by when silver set them aside, which is the same run.
    "rows_removed": """
SELECT feed, bronze, silver, quarantined,
       bronze - silver - quarantined AS removed,
       round(100.0 * (bronze - silver - quarantined) / bronze, 2) AS pct_removed
FROM (
  SELECT 'vehicle_positions' AS feed,
         (SELECT count(*) FROM {c}.{s}.bronze_vehicle_positions WHERE {ingested_in_window}) AS bronze,
         (SELECT count(*) FROM {c}.{s}.silver_vehicle_positions WHERE {ingested_in_window}) AS silver,
         (SELECT count(*) FROM {c}.{s}.silver_quarantine
          WHERE feed = 'vehicle_positions' AND {quarantined_in_window}) AS quarantined
  UNION ALL
  SELECT 'trip_updates',
         (SELECT count(*) FROM {c}.{s}.bronze_trip_updates WHERE {ingested_in_window}),
         (SELECT count(*) FROM {c}.{s}.silver_trip_updates WHERE {ingested_in_window}),
         (SELECT count(*) FROM {c}.{s}.silver_quarantine
          WHERE feed = 'trip_updates' AND {quarantined_in_window})
)""",
    "data_quality": """
SELECT count(DISTINCT run_id) AS runs,
       count(*) AS checks,
       count_if(passed) AS passed,
       round(100.0 * count_if(passed) / count(*), 2) AS pct_checks_passed,
       count(DISTINCT run_id) - count(DISTINCT CASE WHEN NOT passed THEN run_id END) AS runs_all_passed
FROM {c}.{s}.dq_results
WHERE run_id IN (SELECT run_id FROM {c}.{s}.ops_run_log WHERE {started_in_window})""",
    "step_runtime": """
SELECT step,
       count(*) AS attempts,
       count_if(status = 'failed') AS failed,
       percentile(unix_timestamp(finished_at) - unix_timestamp(started_at), 0.5) AS median_s,
       max(unix_timestamp(finished_at) - unix_timestamp(started_at)) AS max_s
FROM {c}.{s}.ops_run_log
WHERE {started_in_window}
GROUP BY step
ORDER BY step""",
    # A run succeeded if its last attempt at every step it logged succeeded.
    "job_runs": """
SELECT count(*) AS runs,
       count_if(failed_steps = 0) AS succeeded,
       round(100.0 * count_if(failed_steps = 0) / count(*), 2) AS pct_succeeded,
       min(first_start) AS first_run,
       max(first_start) AS last_run
FROM (
  SELECT run_id, min(started_at) AS first_start,
         count_if(last_status = 'failed') AS failed_steps
  FROM (
    SELECT run_id, step, started_at,
           max_by(status, started_at) OVER (PARTITION BY run_id, step) AS last_status
    FROM {c}.{s}.ops_run_log
    WHERE run_id <> 'manual'
  )
  GROUP BY run_id
)
WHERE {first_start_in_window}""",
    # Gaps, reported rather than hidden: scheduled runs that never started or did not finish.
    # One row per calendar day, including days with no run at all (a day missing from the log
    # is the worst gap, so it must not drop out). A scheduled slot counts as missed when no run
    # started in that hour; runs started by hand at other hours are counted but fill no slot.
    "run_coverage": """
WITH runs AS (
  SELECT run_id,
         to_date(from_utc_timestamp(min(started_at), '{tz}')) AS run_date,
         hour(from_utc_timestamp(min(started_at), '{tz}')) AS start_hour,
         count_if(last_status = 'failed') AS failed_steps
  FROM (
    SELECT run_id, step, started_at,
           max_by(status, started_at) OVER (PARTITION BY run_id, step) AS last_status
    FROM {c}.{s}.ops_run_log
    WHERE run_id <> 'manual'
  )
  GROUP BY run_id
),
days AS (
  SELECT explode(sequence(greatest(DATE'{start}', min(run_date)),
                          least(DATE'{end}', max(run_date)))) AS run_date
  FROM runs
)
SELECT d.run_date,
       count(r.run_id) AS runs,
       size(array_except(array({scheduled_hours}), collect_list(r.start_hour))) AS missing_runs,
       count_if(r.failed_steps > 0) AS failed_runs,
       count_if(NOT array_contains(array({scheduled_hours}), r.start_hour)) AS off_schedule_runs,
       array_sort(collect_list(r.start_hour)) AS start_hours
FROM days AS d LEFT JOIN runs AS r ON r.run_date = d.run_date
GROUP BY d.run_date
ORDER BY d.run_date""",
}


def metric_sql(
    name: str,
    catalog: str = "workspace",
    schema: str = "transit",
    start: str | None = None,
    end: str | None = None,
) -> str:
    """One metric's SQL with names and the window (inclusive Eastern dates) filled in."""
    start, end = start or ALL_TIME[0], end or ALL_TIME[1]
    for day in (start, end):
        date.fromisoformat(day)  # raises on anything that is not a date, so nothing else reaches SQL

    def window(col: str) -> str:
        return IN_WINDOW.format(col=col, tz=LOCAL_TZ, start=start, end=end)

    return (
        METRIC_QUERIES[name]
        .format(
            c=catalog,
            s=schema,
            tz=LOCAL_TZ,
            start=start,
            end=end,
            scheduled_hours=", ".join(map(str, SCHEDULED_HOURS)),
            started_in_window=window("started_at"),
            first_start_in_window=window("first_start"),
            ingested_in_window=window("_ingested_at"),
            quarantined_in_window=window("_quarantined_at"),
        )
        .strip()
    )


def metric_df(
    spark: SparkSession,
    name: str,
    catalog: str = "workspace",
    schema: str = "transit",
    start: str | None = None,
    end: str | None = None,
) -> DataFrame:
    """Run one metric query."""
    return spark.sql(metric_sql(name, catalog, schema, start, end))


def compute_metrics(
    spark: SparkSession,
    start: str | None = None,
    end: str | None = None,
    catalog: str = "workspace",
    schema: str = "transit",
) -> dict[str, Any]:
    """Headline numbers: every value traces to one query in METRIC_QUERIES.

    With a window, every day in it is a full day and all count toward events per day.
    Without one, the first and last day of the log are usually partial, so the average
    uses the days in between (or every day if there are fewer than 3).
    """

    def df(name: str) -> DataFrame:
        return metric_df(spark, name, catalog, schema, start, end)

    days = [r.asDict() for r in df("events_per_day").collect()]
    full = days if start else (days[1:-1] if len(days) >= 3 else days)
    out: dict[str, Any] = {
        "window": {"start": start, "end": end} if start else None,
        "days_logged": len(days),
        "avg_events_per_full_day": round(sum(d["events"] for d in full) / len(full)) if full else None,
        "min_events_per_day": min((d["events"] for d in full), default=None),
        "max_events_per_day": max((d["events"] for d in full), default=None),
        "total_events": sum(d["events"] for d in days),
    }
    out["freshness"] = df("freshness").first().asDict()
    out["rows_removed"] = {r["feed"]: r.asDict() for r in df("rows_removed").collect()}
    out["data_quality"] = df("data_quality").first().asDict()
    out["job_runs"] = df("job_runs").first().asDict()
    out["step_runtime"] = {r["step"]: r.asDict() for r in df("step_runtime").collect()}
    coverage = [r.asDict() for r in df("run_coverage").collect()]
    out["run_coverage"] = {
        "days": len(coverage),
        "missing_runs": sum(r["missing_runs"] for r in coverage),
        "failed_runs": sum(r["failed_runs"] for r in coverage),
        "off_schedule_runs": sum(r["off_schedule_runs"] for r in coverage),
        "per_day": coverage,
    }
    log.info("metrics: %s", out)
    return out


def _json_default(value: object) -> object:
    return float(value) if isinstance(value, Decimal) else str(value)


def save_metrics(
    spark: SparkSession, metrics: dict[str, Any], catalog: str = "workspace", schema: str = "transit"
) -> None:
    """Append one measurement to ops_metric_results, so each published number has a dated record."""
    window = metrics.get("window") or {}
    row = (
        datetime.now(UTC).replace(tzinfo=None),
        window.get("start"),
        window.get("end"),
        json.dumps(metrics, default=_json_default, sort_keys=True),
    )
    spark.createDataFrame(
        [row], "measured_at timestamp, window_start string, window_end string, metrics string"
    ).write.mode("append").saveAsTable(f"{catalog}.{schema}.ops_metric_results")


def queries_file(start: str, end: str, catalog: str = "workspace", schema: str = "transit") -> str:
    """Every metric query for one window, as the text committed in benchmarks/queries.sql."""
    parts = [
        "-- Generated by `python -m pipelines.metrics`; do not edit by hand.",
        f"-- Measurement window: {start} to {end} (Eastern calendar days, inclusive).",
    ]
    for name in METRIC_QUERIES:
        parts += ["", f"-- {name}", metric_sql(name, catalog, schema, start, end) + ";"]
    return "\n".join(parts) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Print every metric query for the measurement window.")
    p.add_argument("--start", default=MEASUREMENT_WINDOW[0])
    p.add_argument("--end", default=MEASUREMENT_WINDOW[1])
    args = p.parse_args(argv)
    print(queries_file(args.start, args.end), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
