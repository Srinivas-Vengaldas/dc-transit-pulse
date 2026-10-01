"""Tests for the gold marts: schedule times, observed departures, OTP, headways and the incremental build."""
from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from pipelines.gold_marts import (
    GOLD_TABLES,
    build_gold,
    headways,
    route_hour_performance,
    schedule_stops,
    schedule_versions,
    scheduled_ts,
    timepoint_departures,
)

SD = date(2026, 10, 1)  # Eastern Daylight Time: UTC-4
VP_SCHEMA = (
    "vehicle_id string, event_ts timestamp, service_date date, route_id string, trip_id string, "
    "direction_id int, current_stop_sequence int, _silver_loaded_at timestamp"
)
ST_SCHEMA = (
    "feed_version string, trip_id string, stop_id string, stop_sequence int, arrival_secs int, "
    "departure_secs int, timepoint int, shape_dist_traveled double"
)


def utc(h: int, m: int, s: int = 0, day: date = SD) -> datetime:
    return datetime(day.year, day.month, day.day, h, m, s, tzinfo=UTC)


def secs(h: int, m: int, s: int = 0) -> int:
    return h * 3600 + m * 60 + s


def ping(trip: str, seq: int, at: datetime, vehicle: str = "V1", route: str = "A1",
         loaded: datetime | None = None) -> tuple:
    return (vehicle, at, SD, route, trip, 0, seq, loaded or utc(20, 0))


def stop(trip: str, seq: int, dep: int, timepoint: int = 1, stop_id: str | None = None) -> tuple:
    return ("V1", trip, stop_id or f"S{seq}", seq, dep, dep, timepoint, None)


FEED_INFO_SCHEMA = "feed_version string, feed_start_date string"


def naive(dt: datetime) -> datetime:
    """Spark returns timestamps as naive datetimes in the session time zone (UTC in tests)."""
    return dt.replace(tzinfo=None)


def versions(spark):
    return schedule_versions(spark.createDataFrame([("V1", "20260901")], FEED_INFO_SCHEMA))


def departures(spark, pings: list[tuple], stops: list[tuple]):
    vp = spark.createDataFrame(pings, VP_SCHEMA)
    st = schedule_stops(spark.createDataFrame(stops, ST_SCHEMA))
    return timepoint_departures(vp, st, versions(spark))


# --------------------------------------------------------------------------- schedule helpers


def test_scheduled_ts_handles_dst_and_hours_past_24(spark) -> None:
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [(SD, secs(8, 0)), (SD, secs(25, 10)), (date(2026, 12, 1), secs(8, 0))], "sd date, s int"
    )
    got = [r[0] for r in df.select(scheduled_ts(F.col("sd"), F.col("s"))).collect()]
    assert got == [
        naive(utc(12, 0)),  # 08:00 EDT
        naive(utc(5, 10, day=date(2026, 10, 2))),  # 25:10 = 01:10 EDT the next morning
        naive(utc(13, 0, day=date(2026, 12, 1))),  # 08:00 EST
    ]


def test_schedule_versions_are_point_in_time_ranges(spark) -> None:
    df = spark.createDataFrame([("V2", "20270328"), ("V1", "20260913")], FEED_INFO_SCHEMA)
    got = sorted(tuple(r) for r in schedule_versions(df).collect())
    assert got == [("V1", date(2026, 9, 13), date(2027, 3, 28)), ("V2", date(2027, 3, 28), None)]


def test_schedule_stops_uses_timepoints_when_marked_else_all(spark) -> None:
    marked = spark.createDataFrame([stop("T", 1, 0, 1), stop("T", 2, 0, 0)], ST_SCHEMA)
    unmarked = spark.createDataFrame([stop("T", 1, 0, None), stop("T", 2, 0, None)], ST_SCHEMA)
    assert schedule_stops(marked).count() == 1
    assert schedule_stops(unmarked).count() == 2


# --------------------------------------------------------------------------- departures and OTP


def test_departure_is_midpoint_of_bracketing_pings_and_graded(spark) -> None:
    stops = [stop("T1", 1, secs(8, 0)), stop("T1", 2, secs(8, 5), timepoint=0), stop("T1", 3, secs(8, 10)),
             stop("T1", 4, secs(8, 20))]
    pings = [
        ping("T1", 1, utc(12, 0, 10)), ping("T1", 2, utc(12, 0, 40)),  # leaves S1 at ~12:00:25 -> +25 s
        ping("T1", 3, utc(12, 18, 0)), ping("T1", 4, utc(12, 18, 30)),  # leaves S3 at ~12:18:15 -> +8:15
        ping("T1", 5, utc(12, 40, 0)),  # 22 min gap after the last seq-4 ping: too uncertain, S4 not graded
    ]
    got = {r.stop_id: r for r in departures(spark, pings, stops).collect()}
    assert set(got) == {"S1", "S3"}  # S2 is not a timepoint
    assert (got["S1"].delay_s, got["S1"].otp_status, got["S1"].max_error_s) == (25, "on_time", 15)
    assert got["S1"].observed_ts == naive(utc(12, 0, 25))
    assert (got["S3"].delay_s, got["S3"].otp_status) == (495, "late")
    assert got["S1"].hour_local == 8 and got["S1"].day_of_week == "Thu"


@pytest.mark.parametrize(("delay", "status"), [(-121, "early"), (-120, "on_time"), (420, "on_time"),
                                               (421, "late")])
def test_on_time_window_boundaries(spark, delay: int, status: str) -> None:
    observed = utc(12, 0) + timedelta(seconds=delay)
    pings = [ping("T1", 1, observed - timedelta(seconds=10)), ping("T1", 2, observed + timedelta(seconds=10))]
    (row,) = departures(spark, pings, [stop("T1", 1, secs(8, 0))]).collect()
    assert (row.delay_s, row.otp_status) == (delay, status)


def test_trip_first_seen_mid_route_is_not_graded_before_it_was_seen(spark) -> None:
    # The first ping is already past stop 1, so its departure time is unknown.
    pings = [ping("T1", 2, utc(12, 1)), ping("T1", 3, utc(12, 1, 30))]
    got = departures(spark, pings, [stop("T1", 1, secs(8, 0)), stop("T1", 2, secs(8, 1))]).collect()
    assert [r.stop_id for r in got] == ["S2"]


def test_route_hour_performance_counts(spark) -> None:
    stops = [stop(t, 1, secs(8, m)) for t, m in (("T1", 0), ("T2", 10), ("T3", 20))]
    pings = []
    for t, m, late in (("T1", 0, 0), ("T2", 10, 600), ("T3", 20, -300)):
        at = utc(12, m) + timedelta(seconds=late)
        pings += [ping(t, 1, at - timedelta(seconds=15)), ping(t, 2, at + timedelta(seconds=15))]
    (row,) = route_hour_performance(departures(spark, pings, stops)).collect()
    assert (row.departures, row.on_time, row.late, row.early, row.pct_on_time) == (3, 1, 1, 1, 33.3)
    assert row.avg_delay_s == 100.0


def test_headways_flag_bunching_and_gaps(spark) -> None:
    # Scheduled every 10 min; observed at 12:00, 12:02 (bunched with the first) and 12:20 (a gap).
    stops = [stop(t, 1, secs(8, m), stop_id="S1") for t, m in (("T1", 0), ("T2", 10), ("T3", 20))]
    pings = []
    for t, obs in (("T1", utc(12, 0)), ("T2", utc(12, 2)), ("T3", utc(12, 20))):
        pings += [ping(t, 1, obs - timedelta(seconds=15), vehicle=t),
                  ping(t, 2, obs + timedelta(seconds=15), vehicle=t)]
    got = {r.trip_id: r for r in headways(departures(spark, pings, stops)).collect()}
    assert set(got) == {"T2", "T3"}  # the first bus of the day has no headway
    assert (got["T2"].headway_s, got["T2"].sched_headway_s, got["T2"].headway_status) == (120, 600, "bunched")
    assert (got["T3"].headway_s, got["T3"].headway_status) == (1080, "gapped")


# --------------------------------------------------------------------------- incremental build


@pytest.fixture()
def db(spark):
    spark.sql("DROP DATABASE IF EXISTS t_gold CASCADE")
    spark.sql("CREATE DATABASE t_gold")
    spark.createDataFrame([stop("T1", 1, secs(8, 0)), stop("T1", 2, secs(8, 5)), stop("T2", 1, secs(8, 10))],
                          ST_SCHEMA).write.format("delta").saveAsTable("t_gold.silver_stop_times")
    spark.createDataFrame([("V1", "20260901")], FEED_INFO_SCHEMA) \
        .write.format("delta").saveAsTable("t_gold.static_feed_info")
    yield
    spark.sql("DROP DATABASE IF EXISTS t_gold CASCADE")


def add_silver(spark, pings: list[tuple]) -> None:
    spark.createDataFrame(pings, VP_SCHEMA).write.format("delta").mode("append") \
        .saveAsTable("t_gold.silver_vehicle_positions")


def gold_rows(spark) -> dict[str, int]:
    return {t: spark.table(f"t_gold.{t}").count() for t in GOLD_TABLES}


def test_build_gold_is_incremental_and_idempotent(spark, db) -> None:
    add_silver(spark, [ping("T1", 1, utc(12, 0, 10)), ping("T1", 2, utc(12, 0, 40), loaded=utc(20, 0))])
    out = build_gold(spark, catalog="spark_catalog", schema="t_gold")
    assert (out["start"], out["end"]) == (SD, SD)
    first = gold_rows(spark)
    assert first["gold_timepoint_departures"] == 1

    # No new silver rows: nothing is rebuilt and nothing changes.
    assert build_gold(spark, catalog="spark_catalog", schema="t_gold")["start"] is None
    assert gold_rows(spark) == first

    # A later batch adds T2 on the same date: that date is rebuilt, nothing is duplicated.
    add_silver(spark, [ping("T2", 1, utc(12, 10), "V2", loaded=utc(21, 0)),
                       ping("T2", 2, utc(12, 10, 30), "V2", loaded=utc(21, 0))])
    build_gold(spark, catalog="spark_catalog", schema="t_gold")
    deps = spark.table("t_gold.gold_timepoint_departures")
    assert deps.count() == 2
    assert deps.select("trip_id", "stop_id").distinct().count() == 2
    assert spark.table("t_gold.gold_headways").count() == 1

    # A full refresh gives the same rows.
    def snapshot() -> list[tuple]:
        return sorted(tuple(r) for r in spark.table("t_gold.gold_timepoint_departures")
                      .drop("_gold_built_at").collect())

    before = snapshot()
    build_gold(spark, catalog="spark_catalog", schema="t_gold", full_refresh=True)
    after = snapshot()
    assert after == before
