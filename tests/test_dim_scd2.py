"""Tests for the SCD Type 2 route dimension and the point-in-time join."""
from __future__ import annotations

from datetime import date

import pytest

from pipelines.dim_scd2 import DIMS, ENRICHED_VP_VIEW_SQL, apply_scd2, prepare_source

SPEC = DIMS["routes"]
TABLE = "spark_catalog.t_dim.dim_routes"


@pytest.fixture()
def db(spark):
    spark.sql("DROP DATABASE IF EXISTS t_dim CASCADE")
    spark.sql("CREATE DATABASE t_dim")
    yield
    spark.sql("DROP DATABASE IF EXISTS t_dim CASCADE")


def release(spark, version: str, start: date, routes: dict[str, str]):
    df = spark.createDataFrame([(k, v, k) for k, v in routes.items()],
                               "route_id string, route_long_name string, route_short_name string")
    return prepare_source(df, SPEC, version, start)


def rows(spark) -> list[tuple]:
    return sorted((r.route_id, r.route_long_name, r.valid_from, r.valid_to, r.is_current)
                  for r in spark.table(TABLE).collect())


def test_hash_distinguishes_null_positions(spark) -> None:
    df = spark.createDataFrame([("1", "a", None), ("2", None, "a")],
                               "route_id string, route_long_name string, route_short_name string")
    hashes = [r.attr_hash for r in prepare_source(df, SPEC, "V1", date(2026, 1, 1)).collect()]
    assert hashes[0] != hashes[1]


def test_scd2_history_and_idempotency(spark, db) -> None:
    d1, d2 = date(2026, 9, 13), date(2027, 3, 28)
    v1 = {"A1": "Old Name", "B2": "Same", "Z9": "Gone Route"}
    assert apply_scd2(spark, release(spark, "V1", d1, v1), TABLE, SPEC) == {"inserted": 3, "closed": 0}
    assert apply_scd2(spark, release(spark, "V1", d1, v1), TABLE, SPEC) == {"inserted": 0, "closed": 0}

    v2 = {"A1": "New Name", "B2": "Same", "C3": "Brand New"}
    assert apply_scd2(spark, release(spark, "V2", d2, v2), TABLE, SPEC) == {"inserted": 2, "closed": 2}
    expected = [
        ("A1", "New Name", d2, None, True),
        ("A1", "Old Name", d1, d2, False),
        ("B2", "Same", d1, None, True),
        ("C3", "Brand New", d2, None, True),
        ("Z9", "Gone Route", d1, d2, False),
    ]
    assert rows(spark) == expected
    # Reapplying V2 is a no-op too.
    assert apply_scd2(spark, release(spark, "V2", d2, v2), TABLE, SPEC) == {"inserted": 0, "closed": 0}
    assert rows(spark) == expected
    # Exactly one current version per route, and surrogate keys are unique.
    assert spark.table(TABLE).where("is_current").groupBy("route_id").count().where("count > 1").count() == 0
    assert spark.table(TABLE).select("route_sk").distinct().count() == 5


def test_older_release_is_rejected(spark, db) -> None:
    apply_scd2(spark, release(spark, "V2", date(2027, 3, 28), {"A1": "x"}), TABLE, SPEC)
    with pytest.raises(ValueError, match="apply releases in order"):
        apply_scd2(spark, release(spark, "V1", date(2026, 9, 13), {"A1": "y"}), TABLE, SPEC)


def test_point_in_time_join_uses_the_version_in_force(spark, db) -> None:
    d1, d2 = date(2026, 9, 13), date(2027, 3, 28)
    apply_scd2(spark, release(spark, "V1", d1, {"A1": "Old Name"}), TABLE, SPEC)
    apply_scd2(spark, release(spark, "V2", d2, {"A1": "New Name"}), TABLE, SPEC)
    spark.createDataFrame([("1", "A1", date(2027, 3, 27)), ("2", "A1", date(2027, 3, 28)),
                           ("3", "Q7", date(2027, 3, 28)), ("4", None, date(2027, 3, 28))],
                          "vehicle_id string, route_id string, service_date date") \
        .write.format("delta").saveAsTable("spark_catalog.t_dim.silver_vehicle_positions")
    spark.sql(ENRICHED_VP_VIEW_SQL.format(c="spark_catalog", s="t_dim"))
    got = {r.vehicle_id: (r.route_long_name, r.route_match)
           for r in spark.table("spark_catalog.t_dim.silver_vehicle_positions_enriched").collect()}
    assert got == {"1": ("Old Name", "matched"), "2": ("New Name", "matched"), "3": (None, "unknown_route"),
                   "4": (None, "not_on_trip")}
