"""Export small, aggregated snapshots of gold for the public dashboard.

Runs on Databricks as the last job task, after `quality`:

    gold tables --> aggregate SQL --> CSV files + manifest.json in the Volume
                                       --> dashboard/fetch_snapshot.py (laptop) --> dashboard/snapshot/
                                       --> Streamlit app (reads the CSVs, no Spark, no warehouse)

Why a snapshot instead of a live connection: a public dashboard that queries
Databricks would need a running SQL warehouse (compute quota, cost) and a token
stored in a public app. The aggregates are a few hundred KB, so the dashboard
reads files and costs nothing to view. The trade-off is staleness: the snapshot
is as fresh as the last export you fetch.

Write-audit-publish, light: the task depends on `quality`, so a run whose
critical checks fail never publishes a snapshot; the last good one stays.

Every file is written to a temp name and then renamed, so a reader never sees
a half-written CSV.

Usage in a Databricks notebook:
    from pipelines.export_dashboard import export_dashboard
    export_dashboard(spark)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pipelines.gold_marts import DEFAULT_CONFIG

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("export_dashboard")

DEFAULT_OUT_DIR = "/Volumes/workspace/transit/raw/exports/dashboard"

# Weekday rush hours by scheduled hour (Eastern): 6-9 a.m. and 4-7 p.m.
RUSH_HOURS = (6, 7, 8, 16, 17, 18)
WEEKEND = ("Sat", "Sun")

_COMMON = {
    "rush": f"(day_of_week NOT IN {WEEKEND} AND hour_local IN {RUSH_HOURS})",
    "day_type": f"CASE WHEN day_of_week IN {WEEKEND} THEN 'weekend' ELSE 'weekday' END",
}

DATASET_QUERIES: dict[str, str] = {
    # One row per route: overall and rush-hour reliability, delay and bunching.
    "route_summary": """
WITH dep AS (
  SELECT route_id,
         count(*) AS departures,
         count_if(otp_status = 'on_time') AS on_time,
         count_if(otp_status = 'early') AS early,
         count_if(otp_status = 'late') AS late,
         round(avg(delay_s), 1) AS avg_delay_s,
         percentile(delay_s, 0.9) AS p90_delay_s,
         count_if({rush}) AS rush_departures,
         count_if({rush} AND otp_status = 'on_time') AS rush_on_time,
         count(DISTINCT service_date) AS days
  FROM {c}.{s}.gold_timepoint_departures
  GROUP BY route_id
), hw AS (
  SELECT route_id,
         count(*) AS headways,
         count_if(headway_status = 'bunched') AS bunched,
         count_if(headway_status = 'gapped') AS gapped
  FROM {c}.{s}.gold_headways
  GROUP BY route_id
), r AS (
  SELECT route_id, route_short_name, route_long_name FROM {c}.{s}.dim_routes WHERE is_current
)
SELECT dep.route_id, r.route_short_name, r.route_long_name,
       dep.departures, dep.on_time, dep.early, dep.late, dep.avg_delay_s, dep.p90_delay_s,
       dep.rush_departures, dep.rush_on_time, dep.days,
       coalesce(hw.headways, 0) AS headways, coalesce(hw.bunched, 0) AS bunched,
       coalesce(hw.gapped, 0) AS gapped
FROM dep
LEFT JOIN hw ON hw.route_id = dep.route_id
LEFT JOIN r ON r.route_id = dep.route_id
ORDER BY dep.route_id""",
    # Route x weekday/weekend x scheduled hour: counts, so any roll-up is a correct weighted average.
    "route_hour": """
WITH dep AS (
  SELECT route_id, {day_type} AS day_type, hour_local,
         count(*) AS departures,
         count_if(otp_status = 'on_time') AS on_time,
         count_if(otp_status = 'late') AS late,
         count_if(otp_status = 'early') AS early,
         sum(delay_s) AS delay_s_sum
  FROM {c}.{s}.gold_timepoint_departures
  GROUP BY 1, 2, 3
), hw AS (
  SELECT route_id, {day_type} AS day_type, hour_local,
         count(*) AS headways,
         count_if(headway_status = 'bunched') AS bunched,
         count_if(headway_status = 'gapped') AS gapped
  FROM {c}.{s}.gold_headways
  GROUP BY 1, 2, 3
)
SELECT coalesce(dep.route_id, hw.route_id) AS route_id,
       coalesce(dep.day_type, hw.day_type) AS day_type,
       coalesce(dep.hour_local, hw.hour_local) AS hour_local,
       coalesce(departures, 0) AS departures, coalesce(on_time, 0) AS on_time,
       coalesce(late, 0) AS late, coalesce(early, 0) AS early, coalesce(delay_s_sum, 0) AS delay_s_sum,
       coalesce(headways, 0) AS headways, coalesce(bunched, 0) AS bunched, coalesce(gapped, 0) AS gapped
FROM dep
FULL OUTER JOIN hw
  ON dep.route_id = hw.route_id AND dep.day_type = hw.day_type AND dep.hour_local = hw.hour_local
ORDER BY 1, 2, 3""",
    # One row per service date: the trend line and the "how much data is this" answer.
    "daily": """
WITH dep AS (
  SELECT service_date, first(day_of_week) AS day_of_week,
         count(*) AS departures, count_if(otp_status = 'on_time') AS on_time,
         count(DISTINCT route_id) AS routes, count(DISTINCT vehicle_id) AS vehicles
  FROM {c}.{s}.gold_timepoint_departures
  GROUP BY service_date
), hw AS (
  SELECT service_date, count(*) AS headways, count_if(headway_status = 'bunched') AS bunched
  FROM {c}.{s}.gold_headways
  GROUP BY service_date
)
SELECT dep.*, coalesce(hw.headways, 0) AS headways, coalesce(hw.bunched, 0) AS bunched
FROM dep LEFT JOIN hw ON hw.service_date = dep.service_date
ORDER BY service_date""",
    # One row per graded stop with its location: the map.
    "stops": """
WITH dep AS (
  SELECT stop_id,
         count(*) AS departures, count_if(otp_status = 'on_time') AS on_time,
         round(avg(delay_s), 1) AS avg_delay_s,
         count(DISTINCT route_id) AS routes,
         concat_ws(' ', sort_array(collect_set(route_id))) AS route_ids
  FROM {c}.{s}.gold_timepoint_departures
  GROUP BY stop_id
), hw AS (
  SELECT stop_id, count(*) AS headways, count_if(headway_status = 'bunched') AS bunched
  FROM {c}.{s}.gold_headways
  GROUP BY stop_id
)
SELECT dep.stop_id, st.stop_name,
       cast(st.stop_lat AS DOUBLE) AS lat, cast(st.stop_lon AS DOUBLE) AS lon,
       dep.departures, dep.on_time, dep.avg_delay_s, dep.routes, dep.route_ids,
       coalesce(hw.headways, 0) AS headways, coalesce(hw.bunched, 0) AS bunched
FROM dep
LEFT JOIN hw ON hw.stop_id = dep.stop_id
JOIN {c}.{s}.dim_stops AS st ON st.stop_id = dep.stop_id AND st.is_current
ORDER BY dep.stop_id""",
    # Wet vs dry hours, all day and at rush hour: the rain question, with its sample sizes.
    "weather": """
SELECT CASE WHEN is_wet THEN 'wet' ELSE 'dry' END AS weather,
       {rush} AS rush,
       count(DISTINCT service_date, hour_local) AS hours,
       sum(departures) AS departures,
       sum(on_time) AS on_time,
       round(sum(avg_delay_s * departures) / sum(departures), 1) AS avg_delay_s
FROM {c}.{s}.gold_route_hour_weather
GROUP BY 1, 2
ORDER BY 1, 2""",
    # The one stop-day that shows bunching best: every bus there, scheduled vs observed.
    "bunching_example": """
WITH pick AS (
  SELECT service_date, route_id, direction_id, stop_id,
         count_if(headway_status = 'bunched') AS bunched, count(*) AS headways
  FROM {c}.{s}.gold_headways
  GROUP BY 1, 2, 3, 4
  HAVING count(*) >= 6
  ORDER BY bunched DESC, headways DESC, service_date DESC, route_id, stop_id
  LIMIT 1
)
SELECT d.service_date, d.route_id, d.direction_id, d.stop_id, st.stop_name, d.trip_id, d.vehicle_id,
       unix_timestamp(d.scheduled_ts) AS scheduled_epoch, unix_timestamp(d.observed_ts) AS observed_epoch,
       d.delay_s, h.headway_s, h.sched_headway_s, h.headway_status
FROM {c}.{s}.gold_timepoint_departures AS d
JOIN pick AS p
  ON d.service_date = p.service_date AND d.route_id = p.route_id
 AND d.direction_id <=> p.direction_id AND d.stop_id = p.stop_id
LEFT JOIN {c}.{s}.gold_headways AS h
  ON h.service_date = d.service_date AND h.route_id = d.route_id AND h.direction_id <=> d.direction_id
 AND h.stop_id = d.stop_id AND h.trip_id = d.trip_id
LEFT JOIN {c}.{s}.dim_stops AS st ON st.stop_id = d.stop_id AND st.is_current
ORDER BY observed_epoch""",
    # Pipeline health: one row per task attempt, with the few metrics the dashboard plots.
    "pipeline_runs": """
SELECT run_id, step, status,
       unix_timestamp(started_at) AS started_epoch,
       unix_timestamp(finished_at) - unix_timestamp(started_at) AS duration_s,
       coalesce(cast(get_json_object(metrics, '$.vehicle_positions_rows_added') AS BIGINT), 0)
         + coalesce(cast(get_json_object(metrics, '$.trip_updates_rows_added') AS BIGINT), 0) AS bronze_rows,
       cast(get_json_object(metrics, '$.freshness_s') AS INT) AS freshness_s
FROM {c}.{s}.ops_run_log
WHERE run_id <> 'manual'
ORDER BY started_epoch""",
    "dq_checks": """
SELECT check_name, table_name, severity,
       count(*) AS runs, count_if(passed) AS passed,
       sum(failed) AS rows_failed, sum(total) AS rows_checked
FROM {c}.{s}.dq_results
GROUP BY 1, 2, 3
ORDER BY severity, check_name""",
}

# Tables each dataset reads; a dataset is skipped (and listed in the manifest) if one is missing.
DATASET_SOURCES: dict[str, tuple[str, ...]] = {
    "route_summary": ("gold_timepoint_departures", "gold_headways", "dim_routes"),
    "route_hour": ("gold_timepoint_departures", "gold_headways"),
    "daily": ("gold_timepoint_departures", "gold_headways"),
    "stops": ("gold_timepoint_departures", "gold_headways", "dim_stops"),
    "weather": ("gold_route_hour_weather",),
    "bunching_example": ("gold_timepoint_departures", "gold_headways", "dim_stops"),
    "pipeline_runs": ("ops_run_log",),
    "dq_checks": ("dq_results",),
}


def dataset_sql(name: str, catalog: str = "workspace", schema: str = "transit") -> str:
    """One dataset's SQL with names filled in."""
    return DATASET_QUERIES[name].format(c=catalog, s=schema, **_COMMON).strip()


def build_datasets(
    spark: SparkSession, catalog: str = "workspace", schema: str = "transit"
) -> tuple[dict[str, DataFrame], list[str]]:
    """Every dataset whose source tables exist, plus the names that were skipped."""
    out: dict[str, DataFrame] = {}
    skipped: list[str] = []
    for name, sources in DATASET_SOURCES.items():
        if all(spark.catalog.tableExists(f"{catalog}.{schema}.{t}") for t in sources):
            out[name] = spark.sql(dataset_sql(name, catalog, schema))
        else:
            skipped.append(name)
    return out, skipped


def json_default(value: object) -> object:
    """SQL DECIMAL results (round(), percentages) become JSON numbers; anything else (dates) becomes text."""
    return float(value) if isinstance(value, Decimal) else str(value)


def write_atomic(path: Path, text: str) -> None:
    """Write to a temp file beside the target, then rename over it."""
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def export_dashboard(
    spark: SparkSession,
    out_dir: str = DEFAULT_OUT_DIR,
    catalog: str = "workspace",
    schema: str = "transit",
    run_id: str = "manual",
) -> dict[str, Any]:
    """Write every dataset as CSV and a manifest.json describing the snapshot. Returns row counts."""
    from pipelines.metrics import compute_metrics

    target = Path(out_dir)
    target.mkdir(parents=True, exist_ok=True)
    datasets, skipped = build_datasets(spark, catalog, schema)
    rows: dict[str, int] = {}
    for name, df in datasets.items():
        pdf = df.toPandas()
        write_atomic(target / f"{name}.csv", pdf.to_csv(index=False))
        rows[name] = len(pdf)

    window = None
    if "daily" in datasets and rows["daily"]:
        bounds = datasets["daily"].selectExpr("min(service_date)", "max(service_date)").first()
        window = {"first_service_date": str(bounds[0]), "last_service_date": str(bounds[1])}
    manifest = {
        "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "run_id": run_id,
        "service_dates": window,
        "rows": rows,
        "skipped": skipped,
        "definitions": {**asdict(DEFAULT_CONFIG), "rush_hours": list(RUSH_HOURS)},
        "metrics": compute_metrics(spark, catalog, schema) if not skipped else None,
    }
    # The manifest goes last: a reader that sees a new manifest also sees the CSVs it describes.
    write_atomic(target / "manifest.json", json.dumps(manifest, indent=2, default=json_default))
    log.info("dashboard snapshot -> %s: %s (skipped %s)", target, rows, skipped)
    return {"rows": rows, "skipped": skipped}
