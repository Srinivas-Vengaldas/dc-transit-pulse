"""Optimization pass: measure, cluster and compact, measure again.

The brief's small-files problem: every 2-hour run appends a few small files to
each silver and gold table (MERGE, replaceWhere), so after a week a table is
hundreds of small files. Each file costs an open and a footer read, and min/max
statistics per file only help skip data if rows with the same keys sit together.

The fix, in two parts:
  1. Liquid clustering (ALTER TABLE ... CLUSTER BY): Databricks' replacement for
     partitioning + Z-ORDER. Rows are laid out by the clustering keys, so a filter
     on service_date / route_id reads few files. Unlike partitioning it does not
     create a directory per value (no tiny partitions with our ~1 day x 126 routes),
     and the keys can be changed later without rewriting the table by hand.
  2. OPTIMIZE: rewrites small files into large ones and applies the clustering.

How it is measured (before and after, same queries, same session):
  - file count and size per table (DESCRIBE DETAIL): deterministic, cache-proof
  - query time: each benchmark query runs `repeats` times; the median is kept,
    and the first run is reported separately because later runs can hit caches
  - gold step runtime in the scheduled job: ops_run_log, runs before vs after the change
Results go to ops_benchmarks so the before/after numbers are a query away.

Usage in a Databricks notebook:
    from pipelines.optimize import run_benchmark, optimize_tables
    run_benchmark(spark, label="before")
    optimize_tables(spark)
    run_benchmark(spark, label="after")
"""

from __future__ import annotations

import logging
import statistics
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("optimize")

# Clustering keys: what readers filter on. Gold and silver are read by service date and route.
CLUSTER_KEYS: dict[str, tuple[str, ...]] = {
    "silver_vehicle_positions": ("service_date", "route_id"),
    "silver_trip_updates": ("service_date", "route_id"),
    "gold_timepoint_departures": ("service_date", "route_id"),
    "gold_headways": ("service_date", "route_id"),
    "gold_route_hour_performance": ("service_date", "route_id"),
    "gold_route_hour_bunching": ("service_date", "route_id"),
}

BENCH_SCHEMA = (
    "label string, measured_at timestamp, kind string, name string, first_s double, median_s double, "
    "runs int, num_files long, size_bytes long"
)

# Representative reads: a selective lookup (where data skipping matters) and a full aggregate.
BENCH_QUERIES: dict[str, str] = {
    "silver_route_day_lookup": """
SELECT vehicle_id, count(*) AS pings, max(event_ts) AS last_seen
FROM {c}.{s}.silver_vehicle_positions
WHERE service_date = (SELECT max(service_date) FROM {c}.{s}.silver_vehicle_positions)
  AND route_id = (SELECT max_by(route_id, n) FROM (SELECT route_id, count(*) AS n
                  FROM {c}.{s}.silver_vehicle_positions WHERE route_id IS NOT NULL GROUP BY route_id))
GROUP BY vehicle_id""",
    "gold_route_day_lookup": """
SELECT stop_id, count(*) AS departures, avg(delay_s) AS avg_delay_s
FROM {c}.{s}.gold_timepoint_departures
WHERE service_date = (SELECT max(service_date) FROM {c}.{s}.gold_timepoint_departures)
  AND route_id = (SELECT max_by(route_id, n) FROM (SELECT route_id, count(*) AS n
                  FROM {c}.{s}.gold_timepoint_departures GROUP BY route_id))
GROUP BY stop_id""",
    "gold_full_aggregate": """
SELECT route_id, hour_local, count(*) AS departures, count_if(otp_status = 'on_time') AS on_time,
       percentile(delay_s, 0.9) AS p90_delay_s
FROM {c}.{s}.gold_timepoint_departures
GROUP BY route_id, hour_local""",
}


def table_detail(spark: SparkSession, table: str) -> dict:
    """numFiles, sizeInBytes and clusteringColumns from DESCRIBE DETAIL."""
    row = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict()
    return {
        "num_files": int(row.get("numFiles") or 0),
        "size_bytes": int(row.get("sizeInBytes") or 0),
        "clustering": list(row.get("clusteringColumns") or []),
    }


def time_query(spark: SparkSession, sql: str, repeats: int) -> list[float]:
    """Wall-clock seconds for `repeats` runs of a query, results collected to the driver (they are small)."""
    out = []
    for _ in range(repeats):
        start = time.perf_counter()
        spark.sql(sql).collect()
        out.append(round(time.perf_counter() - start, 3))
    return out


def run_benchmark(
    spark: SparkSession,
    label: str,
    catalog: str = "workspace",
    schema: str = "transit",
    repeats: int = 5,
) -> list[dict]:
    """Record file counts for each table and timings for each query under `label` in ops_benchmarks."""
    now = datetime.now(UTC)
    rows: list[dict] = []
    for name in CLUSTER_KEYS:
        d = table_detail(spark, f"{catalog}.{schema}.{name}")
        rows.append(
            {
                "label": label,
                "measured_at": now,
                "kind": "table",
                "name": name,
                "first_s": None,
                "median_s": None,
                "runs": None,
                "num_files": d["num_files"],
                "size_bytes": d["size_bytes"],
            }
        )
    for name, sql in BENCH_QUERIES.items():
        times = time_query(spark, sql.format(c=catalog, s=schema), repeats)
        rows.append(
            {
                "label": label,
                "measured_at": now,
                "kind": "query",
                "name": name,
                "first_s": times[0],
                "median_s": statistics.median(times),
                "runs": len(times),
                "num_files": None,
                "size_bytes": None,
            }
        )
    cols = [c.split()[0] for c in BENCH_SCHEMA.split(", ")]
    spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], BENCH_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(f"{catalog}.{schema}.ops_benchmarks")
    for r in rows:
        log.info(
            "%s %s %s", label, r["name"], {k: r[k] for k in ("num_files", "first_s", "median_s") if r[k]}
        )
    return rows


def optimize_tables(
    spark: SparkSession, catalog: str = "workspace", schema: str = "transit"
) -> dict[str, dict]:
    """Set liquid clustering keys on each table and OPTIMIZE it. Safe to rerun.

    OPTIMIZE FULL reclusters rows written before the keys were set; plain OPTIMIZE
    only clusters new data. Older runtimes lack FULL, so it falls back.
    """
    out = {}
    for name, keys in CLUSTER_KEYS.items():
        table = f"{catalog}.{schema}.{name}"
        before = table_detail(spark, table)
        if before["clustering"] != list(keys):
            spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(keys)})")
        try:
            spark.sql(f"OPTIMIZE {table} FULL")
        except Exception as exc:  # FULL not supported on this runtime
            log.warning("OPTIMIZE FULL failed on %s (%s); running plain OPTIMIZE", table, exc)
            spark.sql(f"OPTIMIZE {table}")
        after = table_detail(spark, table)
        out[name] = {
            "files_before": before["num_files"],
            "files_after": after["num_files"],
            "clustering": after["clustering"],
        }
        log.info("%s: %s", name, out[name])
    return out


COMPARE_SQL = """
SELECT b.kind, b.name,
       b.num_files AS files_before, a.num_files AS files_after,
       b.median_s AS median_s_before, a.median_s AS median_s_after,
       round(100.0 * (b.median_s - a.median_s) / b.median_s, 1) AS pct_faster
FROM (SELECT * FROM {c}.{s}.ops_benchmarks WHERE label = '{before}') AS b
JOIN (SELECT * FROM {c}.{s}.ops_benchmarks WHERE label = '{after}') AS a USING (kind, name)
ORDER BY b.kind DESC, b.name"""


def compare_sql(
    before: str = "before", after: str = "after", catalog: str = "workspace", schema: str = "transit"
) -> str:
    """SQL for the before/after table (assumes one benchmark per label)."""
    return COMPARE_SQL.format(c=catalog, s=schema, before=before, after=after).strip()
