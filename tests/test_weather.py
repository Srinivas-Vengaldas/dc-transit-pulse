"""Tests for the Open-Meteo weather load and the route-hour weather view."""
from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from pipelines.weather import WEATHER_VIEW_SQL, fetch_window, parse_hourly, past_hours, upsert_weather


def payload(times: list[str], precip: list[float | None], temp: float = 20.0) -> dict:
    n = len(times)
    return {"hourly": {"time": times, "temperature_2m": [temp] * n, "precipitation": precip,
                       "rain": precip, "snowfall": [0.0] * n, "weather_code": [61] * n}}


def test_parse_converts_gmt_to_eastern_across_dst() -> None:
    rows = parse_hourly(payload(["2026-10-01T14:00", "2026-12-01T14:00"], [0.0, 1.2]))
    (oct_, dec) = rows
    assert oct_[0] == datetime(2026, 10, 1, 14, tzinfo=UTC)
    assert (oct_[1], oct_[2]) == (date(2026, 10, 1), 10)  # EDT
    assert (dec[1], dec[2]) == (date(2026, 12, 1), 9)  # EST
    assert (oct_[-1], dec[-1]) == (False, True)  # is_wet


def test_parse_skips_hours_without_precipitation_and_handles_midnight() -> None:
    rows = parse_hourly(payload(["2026-10-02T02:00", "2026-10-02T03:00"], [0.1, None]))
    assert len(rows) == 1
    assert (rows[0][1], rows[0][2], rows[0][-1]) == (date(2026, 10, 1), 22, True)  # 22:00 the evening before


def test_future_forecast_hours_are_dropped() -> None:
    hours = ["2026-10-01T21:00", "2026-10-01T22:00", "2026-10-01T23:00"]
    rows = parse_hourly(payload(hours, [0.0, 0.0, 0.0]))
    kept = past_hours(rows, datetime(2026, 10, 1, 22, 30, tzinfo=UTC))
    assert [r[0].hour for r in kept] == [21, 22]


def test_fetch_window_covers_yesterday_and_today_eastern() -> None:
    assert fetch_window(date(2026, 10, 1)) == (date(2026, 9, 30), date(2026, 10, 2))


@pytest.fixture()
def db(spark):
    spark.sql("DROP DATABASE IF EXISTS t_wx CASCADE")
    spark.sql("CREATE DATABASE t_wx")
    yield
    spark.sql("DROP DATABASE IF EXISTS t_wx CASCADE")


def test_upsert_is_idempotent_and_newer_values_win(spark, db) -> None:
    table = "spark_catalog.t_wx.silver_weather_hourly"
    first = parse_hourly(payload(["2026-10-01T14:00", "2026-10-01T15:00"], [0.0, 0.0]))
    upsert_weather(spark, first, table)
    upsert_weather(spark, first, table)
    assert spark.table(table).count() == 2
    # A later run gets a revised value for 15:00 (it rained after all) and a new hour.
    upsert_weather(spark, parse_hourly(payload(["2026-10-01T15:00", "2026-10-01T16:00"], [2.5, 0.0])), table)
    got = {r.local_hour: r.precipitation_mm for r in spark.table(table).collect()}
    assert got == {10: 0.0, 11: 2.5, 12: 0.0}


def test_view_joins_route_hours_to_local_weather(spark, db) -> None:
    upsert_weather(spark, parse_hourly(payload(["2026-10-01T12:00", "2026-10-01T13:00"], [0.0, 3.0])),
                   "spark_catalog.t_wx.silver_weather_hourly")
    spark.createDataFrame([(date(2026, 10, 1), "A1", 8, 50.0), (date(2026, 10, 1), "A1", 9, 80.0)],
                          "service_date date, route_id string, hour_local int, pct_on_time double") \
        .write.format("delta").saveAsTable("spark_catalog.t_wx.gold_route_hour_performance")
    spark.sql(WEATHER_VIEW_SQL.format(c="spark_catalog", s="t_wx"))
    got = {r.hour_local: (r.pct_on_time, r.is_wet)
           for r in spark.table("spark_catalog.t_wx.gold_route_hour_weather").collect()}
    assert got == {8: (50.0, False), 9: (80.0, True)}
