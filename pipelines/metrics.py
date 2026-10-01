"""Pipeline metrics from the run log, data-quality results and table counts.

Every number that goes in the README or on a resume comes from one of the
queries below, so each can be re-run and checked:

    events per day      ops_run_log, bronze step: rows added per Eastern calendar day
    freshness           ops_run_log, gold step: now - newest observed departure, at build time
    rows removed        bronze rows vs silver + quarantine rows, per feed (duplicates + late rows)
    data quality        dq_results: share of checks passed, and runs where every check passed
    step runtime        ops_run_log: finished_at - started_at per step

The queries are plain SQL, so the same text can be pasted into a Databricks SQL
cell; `metric_sql()` prints them with the catalog and schema filled in.

Usage in a Databricks notebook:
    from pipelines.metrics import compute_metrics, metric_sql
    compute_metrics(spark)
    print(metric_sql("events_per_day"))
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("metrics")

LOCAL_TZ = "America/New_York"

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
WHERE step = 'bronze' AND status = 'succeeded'
GROUP BY 1
ORDER BY 1""",
    # Seconds from the newest observed departure to the moment gold finished building it.
    # Scheduled runs only: a manual rebuild hours after the last collect is not a freshness sample.
    "freshness": """
SELECT count(*) AS builds,
       percentile(freshness_s, 0.5) AS median_s,
       percentile(freshness_s, 0.9) AS p90_s,
       max(freshness_s) AS max_s
FROM (
  SELECT cast(get_json_object(metrics, '$.freshness_s') AS INT) AS freshness_s
  FROM {c}.{s}.ops_run_log
  WHERE step = 'gold' AND status = 'succeeded' AND run_id <> 'manual'
)
WHERE freshness_s IS NOT NULL""",
    # Table counts, not run-log sums: a replay or a retried step cannot skew them.
    "rows_removed": """
SELECT feed, bronze, silver, quarantined,
       bronze - silver - quarantined AS removed,
       round(100.0 * (bronze - silver - quarantined) / bronze, 2) AS pct_removed
FROM (
  SELECT 'vehicle_positions' AS feed,
         (SELECT count(*) FROM {c}.{s}.bronze_vehicle_positions) AS bronze,
         (SELECT count(*) FROM {c}.{s}.silver_vehicle_positions) AS silver,
         (SELECT count(*) FROM {c}.{s}.silver_quarantine WHERE feed = 'vehicle_positions') AS quarantined
  UNION ALL
  SELECT 'trip_updates',
         (SELECT count(*) FROM {c}.{s}.bronze_trip_updates),
         (SELECT count(*) FROM {c}.{s}.silver_trip_updates),
         (SELECT count(*) FROM {c}.{s}.silver_quarantine WHERE feed = 'trip_updates')
)""",
    "data_quality": """
SELECT count(DISTINCT run_id) AS runs,
       count(*) AS checks,
       count_if(passed) AS passed,
       round(100.0 * count_if(passed) / count(*), 2) AS pct_checks_passed,
       count(DISTINCT run_id) - count(DISTINCT CASE WHEN NOT passed THEN run_id END) AS runs_all_passed
FROM {c}.{s}.dq_results""",
    "step_runtime": """
SELECT step,
       count(*) AS attempts,
       count_if(status = 'failed') AS failed,
       percentile(unix_timestamp(finished_at) - unix_timestamp(started_at), 0.5) AS median_s,
       max(unix_timestamp(finished_at) - unix_timestamp(started_at)) AS max_s
FROM {c}.{s}.ops_run_log
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
)""",
}


def metric_sql(name: str, catalog: str = "workspace", schema: str = "transit") -> str:
    """One metric's SQL with the catalog and schema filled in (paste into a SQL cell)."""
    return METRIC_QUERIES[name].format(c=catalog, s=schema, tz=LOCAL_TZ).strip()


def metric_df(
    spark: SparkSession, name: str, catalog: str = "workspace", schema: str = "transit"
) -> DataFrame:
    """Run one metric query."""
    return spark.sql(metric_sql(name, catalog, schema))


def compute_metrics(
    spark: SparkSession, catalog: str = "workspace", schema: str = "transit"
) -> dict[str, Any]:
    """Headline numbers: every value traces to one query in METRIC_QUERIES.

    Events per day is the average over *full* days only (first and last day of
    the log are usually partial), and over every day if there are fewer than 3.
    """
    days = [r.asDict() for r in metric_df(spark, "events_per_day", catalog, schema).collect()]
    full = days[1:-1] if len(days) >= 3 else days
    out: dict[str, Any] = {
        "days_logged": len(days),
        "avg_events_per_full_day": round(sum(d["events"] for d in full) / len(full)) if full else None,
        "total_events": sum(d["events"] for d in days),
    }
    out["freshness"] = metric_df(spark, "freshness", catalog, schema).first().asDict()
    out["rows_removed"] = {
        r["feed"]: r.asDict() for r in metric_df(spark, "rows_removed", catalog, schema).collect()
    }
    out["data_quality"] = metric_df(spark, "data_quality", catalog, schema).first().asDict()
    out["job_runs"] = metric_df(spark, "job_runs", catalog, schema).first().asDict()
    out["step_runtime"] = {
        r["step"]: r.asDict() for r in metric_df(spark, "step_runtime", catalog, schema).collect()
    }
    log.info("metrics: %s", out)
    return out
