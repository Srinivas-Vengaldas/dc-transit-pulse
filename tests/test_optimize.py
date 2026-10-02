"""Tests for the optimization pass on local Delta tables (clustering, compaction, benchmark log)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from pipelines.optimize import BENCH_QUERIES, CLUSTER_KEYS, compare_sql, optimize_tables, run_benchmark

DB = "t_opt"
SD = date(2026, 10, 1)


@pytest.fixture(scope="module")
def tables(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    ts = datetime(2026, 10, 1, 12, tzinfo=UTC)
    for name in CLUSTER_KEYS:
        rows = [(SD, f"R{i % 3}", f"V{i}", ts, f"S{i % 2}", 60 * i, i % 24, "on_time") for i in range(12)]
        df = spark.createDataFrame(
            rows,
            "service_date date, route_id string, vehicle_id string, event_ts timestamp, "
            "stop_id string, delay_s int, hour_local int, otp_status string",
        )
        df.write.format("delta").mode("overwrite").saveAsTable(f"{DB}.{name}")
        for row in rows[:3]:  # small appends, like the scheduled runs: many small files
            spark.createDataFrame([row], df.schema).write.format("delta").mode("append").saveAsTable(
                f"{DB}.{name}"
            )
    return spark


def test_optimize_clusters_and_compacts_and_benchmarks_log(tables) -> None:
    before = run_benchmark(tables, "before", catalog="spark_catalog", schema=DB, repeats=2)
    assert {r["name"] for r in before} == set(CLUSTER_KEYS) | set(BENCH_QUERIES)
    out = optimize_tables(tables, catalog="spark_catalog", schema=DB)
    for name, keys in CLUSTER_KEYS.items():
        assert out[name]["clustering"] == list(keys)
        assert out[name]["files_after"] < out[name]["files_before"]
    assert optimize_tables(tables, catalog="spark_catalog", schema=DB)  # rerun is safe
    run_benchmark(tables, "after", catalog="spark_catalog", schema=DB, repeats=2)
    cmp = {r["name"]: r for r in tables.sql(compare_sql(catalog="spark_catalog", schema=DB)).collect()}
    assert cmp["gold_headways"]["files_after"] == 1
    assert cmp["gold_full_aggregate"]["median_s_before"] > 0
    assert tables.table(f"{DB}.silver_vehicle_positions").count() == 15  # optimizing never changes rows
