"""Tests for the pipeline metrics and the dashboard export, on small hand-made gold tables."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from orchestration.run_pipeline import RUN_LOG_SCHEMA
from pipelines.export_dashboard import DATASET_QUERIES, DATASET_SOURCES, build_datasets, export_dashboard
from pipelines.metrics import (
    MEASUREMENT_WINDOW,
    METRIC_QUERIES,
    compute_metrics,
    metric_sql,
    queries_file,
    save_metrics,
)
from pipelines.quality import RESULTS_SCHEMA

DB = "t_dash"
D1, D2 = date(2026, 10, 1), date(2026, 10, 3)  # Thursday, Saturday

DEP_SCHEMA = (
    "service_date date, route_id string, direction_id int, trip_id string, vehicle_id string, "
    "stop_id string, stop_sequence int, scheduled_ts timestamp, observed_ts timestamp, max_error_s int, "
    "hour_local int, "
    "day_of_week string, feed_version string, delay_s int, otp_status string"
)
HW_SCHEMA = (
    "service_date date, day_of_week string, route_id string, direction_id int, stop_id string, "
    "hour_local int, trip_id string, observed_ts timestamp, scheduled_ts timestamp, prev_trip_id string, "
    "headway_s int, "
    "sched_headway_s int, headway_ratio double, headway_status string"
)


def at(d: date, h: int, m: int) -> datetime:
    return datetime(d.year, d.month, d.day, h, m, tzinfo=UTC)


def dep(d: date, route: str, trip: str, stop: str, hour: int, delay: int, status: str) -> tuple:
    sched = at(d, hour + 4, 0)  # Eastern hour -> UTC (EDT)
    return (
        d,
        route,
        0,
        trip,
        "V" + trip,
        stop,
        1,
        sched,
        sched + timedelta(seconds=delay),
        15,
        hour,
        d.strftime("%a"),
        "V1",
        delay,
        status,
    )


def hw(d: date, route: str, stop: str, hour: int, trip: str, status: str) -> tuple:
    t = at(d, hour + 4, 0)
    return (d, d.strftime("%a"), route, 0, stop, hour, trip, t, t, "prev", 60, 600, 0.1, status)


def save(spark, name: str, rows: list[tuple], schema: str) -> None:
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(f"{DB}.{name}")


@pytest.fixture(scope="module")
def tables(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    save(
        spark,
        "gold_timepoint_departures",
        [
            dep(D1, "A1", "t1", "S1", 7, 30, "on_time"),  # weekday rush
            dep(D1, "A1", "t2", "S1", 7, 600, "late"),  # weekday rush
            dep(D1, "A1", "t3", "S2", 12, -200, "early"),
            dep(D2, "B2", "t4", "S1", 7, 0, "on_time"),  # Saturday: not rush
        ],
        DEP_SCHEMA,
    )
    save(
        spark,
        "gold_headways",
        [
            hw(D1, "A1", "S1", 7, "t2", "bunched"),
            hw(D1, "A1", "S1", 7, "t5", "regular"),
            hw(D2, "B2", "S1", 7, "t4", "gapped"),
        ],
        HW_SCHEMA,
    )
    save(
        spark,
        "dim_routes",
        [("A1", "A1", "Alpha Line", True), ("B2", "B2", "Beta Line", True), ("A1", "A1", "Old Alpha", False)],
        "route_id string, route_short_name string, route_long_name string, is_current boolean",
    )
    save(
        spark,
        "dim_stops",
        [("S1", "First & Main", "38.9", "-77.0", True), ("S2", "Second & Oak", "38.8", "-77.1", True)],
        "stop_id string, stop_name string, stop_lat string, stop_lon string, is_current boolean",
    )
    save(
        spark,
        "gold_route_hour_weather",
        [
            (D1, "Thu", "A1", 7, 2, 1, 315.0, True),
            (D1, "Thu", "A1", 12, 1, 0, -200.0, False),
            (D2, "Sat", "B2", 7, 1, 1, 0.0, False),
        ],
        "service_date date, day_of_week string, route_id string, hour_local int, departures int, "
        "on_time int, avg_delay_s double, is_wet boolean",
    )

    def run(rid: str, step: str, start: datetime, secs: int, status: str, metrics: dict) -> tuple:
        return (rid, step, start, start + timedelta(seconds=secs), status, json.dumps(metrics))

    save(
        spark,
        "ops_run_log",
        [
            run(
                "r1",
                "bronze",
                at(D1, 14, 0),
                30,
                "succeeded",
                {"vehicle_positions_rows_added": 100, "trip_updates_rows_added": 200},
            ),
            run("r1", "gold", at(D1, 14, 5), 20, "succeeded", {"freshness_s": 300}),
            run("r2", "bronze", at(D1, 16, 0), 40, "failed", {"error": "boom"}),
            run(
                "r2",
                "bronze",
                at(D1, 16, 2),
                30,
                "succeeded",
                {"vehicle_positions_rows_added": 10, "trip_updates_rows_added": 5},
            ),
            run("r2", "gold", at(D1, 16, 5), 20, "succeeded", {"freshness_s": 500}),
            run(
                "r3",
                "bronze",
                at(D2, 14, 0),
                30,
                "succeeded",
                {"vehicle_positions_rows_added": 1, "trip_updates_rows_added": 1},
            ),
            run("r3", "gold", at(D2, 14, 5), 25, "failed", {"error": "boom"}),
            run("manual", "gold", at(D2, 15, 0), 5, "succeeded", {"freshness_s": 9999}),
        ],
        RUN_LOG_SCHEMA,
    )
    save(
        spark,
        "dq_results",
        [
            ("r1", D1, D1, "a", "t", "critical", 0, 10, 0.0, True),
            ("r1", D1, D1, "b", "t", "warning", 1, 10, 10.0, False),
            ("r2", D1, D1, "a", "t", "critical", 0, 10, 0.0, True),
            ("r2", D1, D1, "b", "t", "warning", 0, 10, 0.0, True),
        ],
        RESULTS_SCHEMA,
    )
    # D1: 10 positions in, 8 kept, 1 quarantined, 1 removed. D2: 3 in, 3 kept.
    t1, t2 = at(D1, 14, 1), at(D2, 14, 1)
    save(spark, "bronze_vehicle_positions", [(t1,)] * 10 + [(t2,)] * 3, "_ingested_at timestamp")
    save(spark, "silver_vehicle_positions", [(t1,)] * 8 + [(t2,)] * 3, "_ingested_at timestamp")
    save(spark, "bronze_trip_updates", [(t1,)] * 4, "_ingested_at timestamp")
    save(spark, "silver_trip_updates", [(t1,)] * 4, "_ingested_at timestamp")
    save(spark, "silver_quarantine", [("vehicle_positions", t1)], "feed string, _quarantined_at timestamp")
    return spark


def test_every_dataset_names_its_sources() -> None:
    assert set(DATASET_QUERIES) == set(DATASET_SOURCES)
    for name, sources in DATASET_SOURCES.items():
        for table in sources:
            assert f"{{c}}.{{s}}.{table}" in DATASET_QUERIES[name], (name, table)


def test_metric_sql_fills_names() -> None:
    sql = metric_sql("events_per_day", "cat", "sch")
    assert "cat.sch.ops_run_log" in sql and "{" not in sql
    assert set(METRIC_QUERIES) >= {"events_per_day", "freshness", "rows_removed", "data_quality"}


def test_compute_metrics(tables) -> None:
    m = compute_metrics(tables, catalog="spark_catalog", schema=DB)
    assert m["days_logged"] == 2
    assert m["total_events"] == 300 + 15 + 2  # the failed attempt counts nothing
    assert m["avg_events_per_full_day"] == round((315 + 2) / 2)  # fewer than 3 days: every day counts
    assert m["freshness"]["builds"] == 2  # scheduled runs only: the manual rebuild is not a sample
    assert m["freshness"]["median_s"] == pytest.approx(400.0)
    assert m["window"] is None
    assert m["rows_removed"]["vehicle_positions"]["removed"] == 1
    assert float(m["rows_removed"]["vehicle_positions"]["pct_removed"]) == pytest.approx(7.69)
    assert m["rows_removed"]["trip_updates"]["removed"] == 0
    assert m["data_quality"] == {
        "runs": 2,
        "checks": 4,
        "passed": 3,
        "pct_checks_passed": pytest.approx(75.0),
        "runs_all_passed": 1,
    }
    # r1 and r2 succeeded (r2's bronze retry succeeded last); r3's gold failed. "manual" is not a job run.
    assert m["job_runs"]["runs"] == 3 and m["job_runs"]["succeeded"] == 2
    assert m["step_runtime"]["bronze"]["failed"] == 1
    # D1 had 2 of 9 scheduled runs; D2 had 1, and it failed at gold.
    assert (m["run_coverage"]["days"], m["run_coverage"]["missing_runs"]) == (2, 7 + 8)
    assert m["run_coverage"]["failed_runs"] == 1
    assert m["run_coverage"]["per_day"][0]["start_hours"] == [10, 12]  # 14:00 and 16:00 UTC in Eastern


def test_compute_metrics_window(tables) -> None:
    day = D1.isoformat()
    m = compute_metrics(tables, day, day, catalog="spark_catalog", schema=DB)
    assert m["window"] == {"start": day, "end": day}
    assert (m["days_logged"], m["total_events"], m["avg_events_per_full_day"]) == (1, 315, 315)
    assert m["freshness"]["builds"] == 2
    assert m["freshness"]["p95_s"] == pytest.approx(490.0)  # [300, 500] at the 95th percentile
    assert m["rows_removed"]["vehicle_positions"]["bronze"] == 10  # D2's rows are outside the window
    assert float(m["rows_removed"]["vehicle_positions"]["pct_removed"]) == pytest.approx(10.0)
    assert (m["job_runs"]["runs"], m["job_runs"]["succeeded"]) == (2, 2)
    assert (m["run_coverage"]["missing_runs"], m["run_coverage"]["failed_runs"]) == (7, 0)


def test_save_metrics_appends_a_dated_record(tables) -> None:
    m = compute_metrics(tables, D1.isoformat(), D1.isoformat(), catalog="spark_catalog", schema=DB)
    save_metrics(tables, m, catalog="spark_catalog", schema=DB)
    row = tables.table(f"spark_catalog.{DB}.ops_metric_results").first()
    assert (row["window_start"], row["window_end"]) == (D1.isoformat(), D1.isoformat())
    assert json.loads(row["metrics"])["total_events"] == 315


def test_metric_sql_rejects_a_bad_window() -> None:
    with pytest.raises(ValueError):
        metric_sql("freshness", start="2026-10-02' OR '1'='1", end="2026-10-08")


def test_queries_file_matches_committed_copy() -> None:
    committed = Path(__file__).resolve().parents[1] / "benchmarks" / "queries.sql"
    assert committed.read_text() == queries_file(*MEASUREMENT_WINDOW), (
        "benchmarks/queries.sql is stale: run `python -m pipelines.metrics > benchmarks/queries.sql`"
    )


def test_build_datasets(tables) -> None:
    ds, skipped = build_datasets(tables, catalog="spark_catalog", schema=DB)
    assert skipped == []
    routes = {r["route_id"]: r.asDict() for r in ds["route_summary"].collect()}
    a1 = routes["A1"]
    assert (a1["departures"], a1["on_time"], a1["early"], a1["late"]) == (3, 1, 1, 1)
    assert (a1["rush_departures"], a1["rush_on_time"]) == (2, 1)
    assert (a1["headways"], a1["bunched"], a1["gapped"]) == (2, 1, 0)
    assert a1["route_long_name"] == "Alpha Line"  # current version only, no duplicate rows
    assert routes["B2"]["rush_departures"] == 0  # Saturday 7 a.m. is not rush hour

    hours = {(r["route_id"], r["day_type"], r["hour_local"]): r.asDict() for r in ds["route_hour"].collect()}
    assert hours[("A1", "weekday", 7)]["departures"] == 2 and hours[("A1", "weekday", 7)]["bunched"] == 1
    assert hours[("B2", "weekend", 7)]["gapped"] == 1

    stops = {r["stop_id"]: r.asDict() for r in ds["stops"].collect()}
    assert stops["S1"]["lat"] == pytest.approx(38.9) and stops["S1"]["route_ids"] == "A1 B2"

    weather = {(r["weather"], r["rush"]): r.asDict() for r in ds["weather"].collect()}
    assert weather[("wet", True)]["departures"] == 2 and weather[("wet", True)]["hours"] == 1
    assert weather[("dry", False)]["departures"] == 2

    example = ds["bunching_example"].collect()
    assert example == []  # no stop-day has the 6 headways needed to show bunching

    runs = ds["pipeline_runs"].collect()
    assert all(r["run_id"] != "manual" for r in runs)


def test_build_datasets_skips_missing_tables(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS t_dash_empty")
    ds, skipped = build_datasets(spark, catalog="spark_catalog", schema="t_dash_empty")
    assert ds == {} and set(skipped) == set(DATASET_SOURCES)


def test_export_writes_csvs_and_manifest(tables, tmp_path: Path) -> None:
    out = export_dashboard(tables, out_dir=str(tmp_path), catalog="spark_catalog", schema=DB, run_id="r9")
    assert out["skipped"] == []
    for name in DATASET_SOURCES:
        assert (tmp_path / f"{name}.csv").exists()
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["run_id"] == "r9"
    assert manifest["service_dates"] == {"first_service_date": str(D1), "last_service_date": str(D2)}
    assert manifest["rows"]["route_summary"] == 2
    assert manifest["definitions"]["late_s"] == 420
    assert manifest["metrics"]["data_quality"]["checks"] == 4
    assert manifest["metrics"]["data_quality"]["pct_checks_passed"] == 75.0  # a JSON number, not "75.00"
    assert not list(tmp_path.glob(".*.tmp"))  # temp files were renamed away
