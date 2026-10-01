"""Data-quality checks on silver and gold, run as the last job task.

Each check is one SQL aggregate that counts bad rows in the service dates the
latest gold build covered:

    SELECT count_if(<row is bad>) AS failed, count(*) AS total
    FROM <table rows in the date range>

A check passes when failed / total <= max_fail_pct. Results for every check are
appended to dq_results (one row per check per run), which gives the brief's
"data-quality pass rate" metric. If a *critical* check fails, the task raises
after writing the results, so the job run shows red and sends its alert;
*warning* checks are recorded but never fail the run.

Why plain SQL instead of a framework: each rule reads like the business rule it
encodes, runs on serverless Spark Connect, and is tested in CI like the rest.

Usage in a Databricks notebook:
    from pipelines.quality import run_checks
    run_checks(spark)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("quality")

RESULTS_SCHEMA = (
    "run_id string, start_date date, end_date date, check_name string, table_name string, severity string, "
    "failed long, total long, fail_pct double, passed boolean"
)


class DataQualityError(RuntimeError):
    """Raised when a critical check fails."""


@dataclass(frozen=True)
class Check:
    """One rule: rows of `table` where `bad` is true are failures."""

    name: str
    table: str
    bad: str  # SQL boolean expression, true for a failing row
    severity: str = "critical"  # or "warning"
    max_fail_pct: float = 0.0

    def sql(self, c: str, s: str, start: date, end: date) -> str:
        """Flag each row in the date range first, then count: window rules (duplicates) need that order."""
        flagged = (
            f"SELECT ({self.bad}) AS _bad FROM {c}.{s}.{self.table} "
            f"WHERE service_date BETWEEN DATE'{start}' AND DATE'{end}'"
        )
        return f"SELECT count_if(_bad) AS failed, count(*) AS total FROM ({flagged})"


def duplicate_key(cols: str) -> str:
    """A row is bad if another row has the same key (window count > 1)."""
    return f"count(*) OVER (PARTITION BY {cols}) > 1"


CHECKS: tuple[Check, ...] = (
    # Silver: the inputs gold relies on.
    Check("vp_vehicle_id_present", "silver_vehicle_positions", "vehicle_id IS NULL"),
    Check("vp_key_unique", "silver_vehicle_positions", duplicate_key("vehicle_id, event_ts")),
    Check("vp_in_dc_region", "silver_vehicle_positions",
          "NOT (latitude BETWEEN 38.5 AND 39.5 AND longitude BETWEEN -77.7 AND -76.6)",
          severity="warning", max_fail_pct=0.5),
    Check("tu_key_unique", "silver_trip_updates", duplicate_key("trip_id, trip_start_date, event_ts")),
    Check("tu_delay_plausible", "silver_trip_updates", "abs(delay_s) > 7200",
          severity="warning", max_fail_pct=0.5),
    # Gold: the numbers that get reported.
    Check("dep_key_unique", "gold_timepoint_departures",
          duplicate_key("trip_id, service_date, stop_sequence")),
    Check("dep_error_bounded", "gold_timepoint_departures", "max_error_s > 60 OR max_error_s < 0"),
    Check("dep_status_valid", "gold_timepoint_departures",
          "otp_status IS NULL OR otp_status NOT IN ('early', 'on_time', 'late')"),
    Check("dep_delay_plausible", "gold_timepoint_departures", "abs(delay_s) > 10800",
          severity="warning", max_fail_pct=1.0),
    Check("perf_counts_add_up", "gold_route_hour_performance", "on_time + early + late <> departures"),
    Check("perf_pct_in_range", "gold_route_hour_performance", "pct_on_time < 0 OR pct_on_time > 100"),
    Check("headway_non_negative", "gold_headways", "headway_s < 0"),
)


RESULT_COLUMNS = [c.split()[0] for c in RESULTS_SCHEMA.split(", ")]


def evaluate(spark: SparkSession, check: Check, catalog: str, schema: str, start: date, end: date) -> dict:
    """Run one check; returns its result as a dict keyed like RESULTS_SCHEMA (without run_id)."""
    failed, total = spark.sql(check.sql(catalog, schema, start, end)).first()
    pct = round(100.0 * failed / total, 3) if total else 0.0
    return {
        "start_date": start, "end_date": end, "check_name": check.name, "table_name": check.table,
        "severity": check.severity, "failed": int(failed), "total": int(total), "fail_pct": pct,
        "passed": pct <= check.max_fail_pct,
    }


def latest_build_range(spark: SparkSession, catalog: str, schema: str) -> tuple[date, date] | None:
    """Service-date range of the most recent gold build, or None before the first build."""
    table = f"{catalog}.{schema}.gold_build_log"
    if not spark.catalog.tableExists(table):
        return None
    row = spark.table(table).orderBy("built_at", ascending=False).select("start_date", "end_date").first()
    return None if row is None else (row[0], row[1])


def run_checks(
    spark: SparkSession,
    catalog: str = "workspace",
    schema: str = "transit",
    run_id: str = "manual",
    checks: tuple[Check, ...] = CHECKS,
) -> dict[str, int]:
    """Run every check on the latest gold build's dates, record results, raise on critical failures."""
    rng = latest_build_range(spark, catalog, schema)
    if rng is None:
        log.info("no gold build yet; nothing to check")
        return {"checks": 0, "passed": 0, "critical_failed": 0}
    start, end = rng
    results = [{"run_id": run_id, **evaluate(spark, ch, catalog, schema, start, end)} for ch in checks]
    spark.createDataFrame([tuple(r[c] for c in RESULT_COLUMNS) for r in results], RESULTS_SCHEMA) \
        .write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.dq_results")

    def failing(severity: str) -> list[str]:
        return [r["check_name"] for r in results if not r["passed"] and r["severity"] == severity]

    failed_critical = failing("critical")
    summary = {"checks": len(results), "passed": sum(r["passed"] for r in results),
               "critical_failed": len(failed_critical), "warnings": len(failing("warning"))}
    log.info("data quality %s..%s: %s", start, end, summary)
    if failed_critical:
        raise DataQualityError(f"critical checks failed: {', '.join(failed_critical)}")
    return summary
