"""Tests for the data-quality checks."""
from __future__ import annotations

from datetime import date

import pytest

from pipelines.quality import CHECKS, Check, DataQualityError, run_checks

SD = date(2026, 10, 1)


@pytest.fixture()
def db(spark):
    spark.sql("DROP DATABASE IF EXISTS t_dq CASCADE")
    spark.sql("CREATE DATABASE t_dq")
    spark.createDataFrame([(SD, SD)], "start_date date, end_date date") \
        .selectExpr("*", "current_timestamp() AS built_at") \
        .write.format("delta").saveAsTable("t_dq.gold_build_log")
    yield
    spark.sql("DROP DATABASE IF EXISTS t_dq CASCADE")


def table(spark, name: str, rows: list[tuple], schema: str) -> None:
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(f"t_dq.{name}")


def run(spark, checks: tuple[Check, ...]) -> dict:
    return run_checks(spark, catalog="spark_catalog", schema="t_dq", run_id="r1", checks=checks)


def test_duplicate_key_fails_a_critical_check_and_results_are_recorded(spark, db) -> None:
    table(spark, "gold_headways", [(SD, "T1", 60), (SD, "T1", 60), (SD, "T2", 90)],
          "service_date date, trip_id string, headway_s int")
    dup = Check("hw_unique", "gold_headways", "count(*) OVER (PARTITION BY trip_id) > 1")
    with pytest.raises(DataQualityError, match="hw_unique"):
        run(spark, (dup,))
    (r,) = spark.table("t_dq.dq_results").collect()
    assert (r.check_name, r.failed, r.total, r.passed, r.run_id) == ("hw_unique", 2, 3, False, "r1")


def test_warning_threshold_and_date_scope(spark, db) -> None:
    # One bad row in range (1 of 4 = 25%), one bad row outside the build's dates (ignored).
    table(spark, "gold_headways", [(SD, -1), (SD, 10), (SD, 20), (SD, 30), (date(2026, 9, 1), -5)],
          "service_date date, headway_s int")
    loose = Check("hw_warn", "gold_headways", "headway_s < 0", severity="warning", max_fail_pct=30.0)
    strict = Check("hw_warn_strict", "gold_headways", "headway_s < 0", severity="warning", max_fail_pct=10.0)
    out = run(spark, (loose, strict))
    assert out == {"checks": 2, "passed": 1, "critical_failed": 0, "warnings": 1}
    got = {r.check_name: (r.failed, r.total, r.fail_pct) for r in spark.table("t_dq.dq_results").collect()}
    assert got == {"hw_warn": (1, 4, 25.0), "hw_warn_strict": (1, 4, 25.0)}


def test_no_gold_build_yet_means_nothing_to_check(spark) -> None:
    spark.sql("DROP DATABASE IF EXISTS t_dq_empty CASCADE")
    spark.sql("CREATE DATABASE t_dq_empty")
    out = run_checks(spark, catalog="spark_catalog", schema="t_dq_empty")
    assert out["checks"] == 0
    spark.sql("DROP DATABASE t_dq_empty CASCADE")


def test_check_catalog_is_well_formed() -> None:
    names = [c.name for c in CHECKS]
    assert len(names) == len(set(names))
    assert all(c.severity in ("critical", "warning") for c in CHECKS)
    assert all(c.max_fail_pct == 0.0 for c in CHECKS if c.severity == "critical")


def test_every_real_check_runs_and_passes_on_clean_tables(spark, db) -> None:
    from datetime import UTC, datetime

    ts = datetime(2026, 10, 1, 12, tzinfo=UTC)
    table(spark, "silver_vehicle_positions", [(SD, "V1", ts, 38.9, -77.0)],
          "service_date date, vehicle_id string, event_ts timestamp, latitude double, longitude double")
    table(spark, "silver_trip_updates", [(SD, "T1", SD, ts, 60)],
          "service_date date, trip_id string, trip_start_date date, event_ts timestamp, delay_s int")
    table(spark, "gold_timepoint_departures", [(SD, "T1", 1, 15, "on_time", 60)],
          "service_date date, trip_id string, stop_sequence int, max_error_s int, otp_status string, "
          "delay_s int")
    table(spark, "gold_route_hour_performance", [(SD, 1, 0, 0, 1, 100.0)],
          "service_date date, on_time int, early int, late int, departures int, pct_on_time double")
    table(spark, "gold_headways", [(SD, 300)], "service_date date, headway_s int")
    out = run(spark, CHECKS)
    assert out == {"checks": len(CHECKS), "passed": len(CHECKS), "critical_failed": 0, "warnings": 0}
