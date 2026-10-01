"""Tests for silver parsing, data-quality rules, watermark dedupe and the idempotent MERGE."""
from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from pipelines.silver_transform import (
    FEEDS,
    checkpoint_paths,
    merge_condition,
    reconcile,
    reset_silver,
    run_silver_feed,
)

FETCHED = datetime(2026, 10, 1, 4, 31, tzinfo=UTC)


def epoch(*args: int) -> str:
    return str(int(datetime(*args, tzinfo=UTC).timestamp()))


def vp(vehicle_id: str | None, ts: str | None, lat: float = 38.9, lon: float = -77.03,
       trip: dict | None = None, **extra: object) -> dict:
    vehicle: dict = {"position": {"latitude": lat, "longitude": lon}, "current_status": "STOPPED_AT"}
    if vehicle_id is not None:
        vehicle["vehicle"] = {"id": vehicle_id, "label": f"L{vehicle_id}"}
    if ts is not None:
        vehicle["timestamp"] = ts
    if trip is not None:
        vehicle["trip"] = trip
    vehicle.update(extra)
    return {"id": vehicle_id or "x", "vehicle": vehicle}


def tu(trip_id: str | None, ts: str | None, delay: int | None = 60, n_stops: int = 2) -> dict:
    trip_update: dict = {"trip": {"start_date": "20260930", "route_id": "A1", "direction_id": 1},
                         "stop_time_update": [{"stop_id": str(i)} for i in range(n_stops)]}
    if trip_id is not None:
        trip_update["trip"]["trip_id"] = trip_id
    if ts is not None:
        trip_update["timestamp"] = ts
    if delay is not None:
        trip_update["delay"] = delay
    return {"id": trip_id or "x", "trip_update": trip_update}


def bronze_df(spark, feed: str, entities: list[dict], fetched_at: datetime = FETCHED,
              entity_as_string: bool = False):
    """A DataFrame shaped like Auto Loader's bronze output (all leaf values are strings).

    entity_as_string=True mimics the shape seen on Databricks, where `entity` is one JSON string.
    """
    from pyspark.sql import functions as F

    header = str(int(fetched_at.timestamp()) - 5)
    lines = [json.dumps({"feed": feed, "feed_header_ts": header, "fetched_at": fetched_at.isoformat(),
                         "entity": json.dumps(e) if entity_as_string else e}) for e in entities]
    df = spark.read.option("primitivesAsString", "true").json(spark.sparkContext.parallelize(lines))
    return (df.withColumn("_source_file", F.lit(f"/landing/{feed}/{feed}_{header}.jsonl"))
            .withColumn("_ingested_at", F.current_timestamp()))


# --------------------------------------------------------------------------- no Spark needed


def test_merge_condition_is_null_safe_on_every_key_column() -> None:
    assert merge_condition(("trip_id", "event_ts")) == "t.trip_id <=> s.trip_id AND t.event_ts <=> s.event_ts"


def test_checkpoints_are_per_feed_and_per_stream() -> None:
    assert checkpoint_paths("/Volumes/w/t/raw/", "trip_updates") == {
        "silver": "/Volumes/w/t/raw/_checkpoints/silver_trip_updates",
        "quarantine": "/Volumes/w/t/raw/_checkpoints/silver_quarantine_trip_updates",
    }


def test_dedupe_keys() -> None:
    assert FEEDS["vehicle_positions"].key == ("vehicle_id", "event_ts")
    assert FEEDS["trip_updates"].key == ("trip_id", "trip_start_date", "event_ts")


def test_unknown_feed_rejected() -> None:
    with pytest.raises(ValueError, match="unknown feed"):
        run_silver_feed(spark=None, feed="alerts", raw_volume="/x")  # type: ignore[arg-type]


# --------------------------------------------------------------------------- parsing


def test_vehicle_positions_are_typed(spark) -> None:
    trip = {"trip_id": "T1", "route_id": "A1", "direction_id": 1, "start_date": "20260930",
            "start_time": "24:10:00"}
    df = bronze_df(spark, "vehicle_positions", [vp("100", epoch(2026, 10, 1, 4, 30), trip=trip,
                                                    current_stop_sequence=7)])
    from pyspark.sql import functions as F

    row = FEEDS["vehicle_positions"].parse(df).withColumn("epoch", F.col("event_ts").cast("long")).first()
    assert row.vehicle_id == "100"
    assert str(row.epoch) == epoch(2026, 10, 1, 4, 30)
    assert row.direction_id == 1 and row.current_stop_sequence == 7
    assert row.latitude == pytest.approx(38.9) and row.longitude == pytest.approx(-77.03)
    assert row.staleness_s == 60
    # 00:30 Eastern on Oct 1, but the trip started on the Sep 30 service day.
    assert row.service_date == date(2026, 9, 30)
    assert row._dq_reason is None


def test_entity_stored_as_json_string_is_parsed(spark) -> None:
    trip = {"trip_id": "T1", "route_id": "A1", "direction_id": 1, "start_date": "20260930"}
    vps = bronze_df(spark, "vehicle_positions", [vp("100", epoch(2026, 10, 1, 4, 30), trip=trip)],
                    entity_as_string=True)
    assert dict(vps.dtypes)["entity"] == "string"
    row = FEEDS["vehicle_positions"].parse(vps).first()
    assert (row.vehicle_id, row.route_id, row.direction_id, row._dq_reason) == ("100", "A1", 1, None)
    assert row.latitude == pytest.approx(38.9)
    tus = bronze_df(spark, "trip_updates", [tu("T1", epoch(2026, 10, 1, 4, 30), delay=-45, n_stops=3)],
                    entity_as_string=True)
    row = FEEDS["trip_updates"].parse(tus).first()
    assert (row.trip_id, row.delay_s, row.n_stop_time_updates, row._dq_reason) == ("T1", -45, 3, None)


def test_service_date_without_trip_is_the_eastern_calendar_date(spark) -> None:
    df = bronze_df(spark, "vehicle_positions", [vp("100", epoch(2026, 10, 1, 2, 0))])
    row = FEEDS["vehicle_positions"].parse(df).first()
    assert row.service_date == date(2026, 9, 30)
    assert row.trip_id is None


def test_fields_never_seen_in_bronze_become_null(spark) -> None:
    df = bronze_df(spark, "vehicle_positions", [vp("100", epoch(2026, 10, 1, 4, 30))])
    assert "bearing" not in json.dumps(df.schema.jsonValue())
    row = FEEDS["vehicle_positions"].parse(df).first()
    assert row.bearing is None and row.occupancy_status is None


@pytest.mark.parametrize(
    ("entity", "reason"),
    [
        (vp(None, epoch(2026, 10, 1, 4, 30)), "missing_vehicle_id"),
        (vp("1", "not-a-number"), "missing_or_bad_timestamp"),
        (vp("1", None), "missing_or_bad_timestamp"),
        (vp("1", epoch(2026, 10, 1, 4, 30), lat=0.0, lon=0.0), "invalid_position"),
        (vp("1", epoch(2026, 10, 1, 4, 30), lat=95.0), "invalid_position"),
        (vp("1", epoch(2026, 10, 1, 5, 0)), "timestamp_in_future"),
    ],
)
def test_vehicle_position_quality_rules(spark, entity: dict, reason: str) -> None:
    df = bronze_df(spark, "vehicle_positions", [entity, vp("ok", epoch(2026, 10, 1, 4, 30))])
    reasons = {r.vehicle_id: r._dq_reason for r in FEEDS["vehicle_positions"].parse(df).collect()}
    assert reasons["ok"] is None
    assert reason in reasons.values()


def test_trip_updates_are_typed(spark) -> None:
    df = bronze_df(spark, "trip_updates", [tu("T1", epoch(2026, 10, 1, 4, 30), delay=-45, n_stops=3)])
    row = FEEDS["trip_updates"].parse(df).first()
    assert (row.trip_id, row.route_id, row.direction_id) == ("T1", "A1", 1)
    assert row.delay_s == -45 and row.n_stop_time_updates == 3
    assert row.trip_start_date == date(2026, 9, 30) == row.service_date
    assert row._dq_reason is None


def test_trip_update_without_timestamp_uses_feed_header_time(spark) -> None:
    df = bronze_df(spark, "trip_updates", [tu("T1", None), tu(None, epoch(2026, 10, 1, 4, 30))])
    rows = {r.trip_id: r for r in FEEDS["trip_updates"].parse(df).collect()}
    assert rows["T1"].event_ts == rows["T1"].feed_header_ts
    assert rows[None]._dq_reason == "missing_trip_id"


# --------------------------------------------------------------------------- end to end


@pytest.fixture()
def lake(spark, tmp_path: Path):
    spark.sql("DROP DATABASE IF EXISTS t_silver CASCADE")
    spark.sql("CREATE DATABASE t_silver")
    yield {"catalog": "spark_catalog", "schema": "t_silver", "raw_volume": str(tmp_path / "raw")}
    spark.sql("DROP DATABASE IF EXISTS t_silver CASCADE")


def append_bronze(spark, feed: str, entities: list[dict], fetched_at: datetime = FETCHED) -> None:
    bronze_df(spark, feed, entities, fetched_at).write.format("delta").mode("append").option(
        "mergeSchema", "true").saveAsTable(f"spark_catalog.t_silver.bronze_{feed}")


def count(spark, table: str) -> int:
    return spark.table(f"spark_catalog.t_silver.{table}").count()


def test_silver_dedupes_quarantines_and_is_idempotent(spark, lake) -> None:
    t0 = epoch(2026, 10, 1, 4, 30)
    # One poll: vehicle 1 twice (a repeated ping), vehicle 2, and one bad row.
    append_bronze(spark, "vehicle_positions", [vp("1", t0), vp("1", t0), vp("2", t0), vp(None, t0)])
    first = run_silver_feed(spark, "vehicle_positions", **lake)
    assert first == {"bronze_rows_read": 4, "silver_rows_added": 2, "quarantine_rows_added": 1}
    q = spark.table("spark_catalog.t_silver.silver_quarantine").first()
    assert (q.feed, q.reason) == ("vehicle_positions", "missing_vehicle_id")

    # Rerun with no new bronze rows: nothing read, nothing added.
    assert run_silver_feed(spark, "vehicle_positions", **lake) == {
        "bronze_rows_read": 0, "silver_rows_added": 0, "quarantine_rows_added": 0}
    assert count(spark, "silver_vehicle_positions") == 2

    # Next poll 40 minutes later: vehicle 1 repeats its old (stale) ping, vehicle 3 is new.
    later = datetime(2026, 10, 1, 5, 11, tzinfo=UTC)
    append_bronze(spark, "vehicle_positions", [vp("1", t0), vp("3", epoch(2026, 10, 1, 5, 10))], later)
    run_silver_feed(spark, "vehicle_positions", **lake)
    assert sorted(r.vehicle_id for r in spark.table("spark_catalog.t_silver.silver_vehicle_positions")
                  .collect()) == ["1", "2", "3"]
    assert reconcile(spark, "vehicle_positions", "spark_catalog", "t_silver") == {
        "bronze_rows": 6, "quarantined": 1, "duplicates": 2, "dropped_late": 0, "silver_rows": 3,
        "pct_duplicates": 33.33}


def test_replay_after_lost_checkpoint_adds_no_duplicates(spark, lake) -> None:
    t0 = epoch(2026, 10, 1, 4, 30)
    append_bronze(spark, "vehicle_positions", [vp("1", t0), vp("2", t0)])
    run_silver_feed(spark, "vehicle_positions", **lake)
    shutil.rmtree(checkpoint_paths(lake["raw_volume"], "vehicle_positions")["silver"])
    replay = run_silver_feed(spark, "vehicle_positions", **lake)
    assert replay["bronze_rows_read"] == 2
    assert replay["silver_rows_added"] == 0


def test_late_event_behind_watermark_is_dropped(spark, lake) -> None:
    append_bronze(spark, "vehicle_positions", [vp("1", epoch(2026, 10, 1, 5, 0))],
                  datetime(2026, 10, 1, 5, 1, tzinfo=UTC))
    run_silver_feed(spark, "vehicle_positions", watermark_delay="10 minutes", **lake)
    # Watermark is now 04:50; a first-seen ping stamped 04:40 is late, one at 04:55 is not.
    append_bronze(spark, "vehicle_positions", [vp("2", epoch(2026, 10, 1, 4, 40)),
                                               vp("3", epoch(2026, 10, 1, 4, 55))],
                  datetime(2026, 10, 1, 5, 2, tzinfo=UTC))
    run_silver_feed(spark, "vehicle_positions", watermark_delay="10 minutes", **lake)
    silver = spark.table("spark_catalog.t_silver.silver_vehicle_positions")
    assert sorted(r.vehicle_id for r in silver.collect()) == ["1", "3"]
    rec = reconcile(spark, "vehicle_positions", "spark_catalog", "t_silver")
    assert (rec["bronze_rows"], rec["dropped_late"], rec["silver_rows"]) == (3, 1, 2)


def test_trip_updates_end_to_end(spark, lake) -> None:
    t0 = epoch(2026, 10, 1, 4, 30)
    append_bronze(spark, "trip_updates", [tu("T1", t0), tu("T1", t0), tu("T2", t0)])
    run_silver_feed(spark, "trip_updates", **lake)
    run_silver_feed(spark, "trip_updates", **lake)
    assert count(spark, "silver_trip_updates") == 2


def test_dropping_only_the_table_is_caught_and_reset_rebuilds(spark, lake) -> None:
    t0 = epoch(2026, 10, 1, 4, 30)
    append_bronze(spark, "vehicle_positions", [vp("1", t0), vp("2", t0)])
    run_silver_feed(spark, "vehicle_positions", **lake)
    spark.sql("DROP TABLE spark_catalog.t_silver.silver_vehicle_positions")
    with pytest.raises(RuntimeError, match="reset_silver"):
        run_silver_feed(spark, "vehicle_positions", **lake)

    reset_silver(spark, lake["raw_volume"], lake["catalog"], lake["schema"])
    assert run_silver_feed(spark, "vehicle_positions", **lake)["silver_rows_added"] == 2
