"""Hourly Washington, DC weather from Open-Meteo, joined to the gold route-hour marts.

Runs on Databricks as the `weather` job task, in parallel with collect/bronze/silver:

    Open-Meteo (free, no key) --> silver_weather_hourly --> view gold_route_hour_weather
                                                           (gold_route_hour_performance + weather)

Each run fetches yesterday and today (Eastern), so a day's hours are fetched
again on the next runs. Open-Meteo fills recent hours with model estimates and
later with better data, so the write is an upsert (MERGE on hour_utc): the
newest value replaces the older one, and rerunning changes nothing.

The view answers the brief's question "do delays rise with rain?" by joining
each route-hour to the weather in that local hour.

Usage in a Databricks notebook:
    from pipelines.weather import load_weather
    load_weather(spark)
"""
from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    import requests
    from pyspark.sql import SparkSession

log = logging.getLogger("weather")

LOCAL_TZ = ZoneInfo("America/New_York")
DC_LAT, DC_LON = 38.9072, -77.0369  # downtown Washington, DC
API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = ("temperature_2m", "precipitation", "rain", "snowfall", "weather_code")
WET_HOUR_MM = 0.1  # an hour with at least this much precipitation counts as wet

WEATHER_SCHEMA = (
    "hour_utc timestamp, local_date date, local_hour int, temperature_c double, precipitation_mm double, "
    "rain_mm double, snowfall_cm double, weather_code int, is_wet boolean"
)


def fetch_hourly(
    session: requests.Session, start: date, end: date, timeout_s: float = 20.0
) -> dict[str, Any]:
    """Raw Open-Meteo hourly response for [start, end] (UTC dates, inclusive)."""
    resp = session.get(
        API_URL,
        params={
            "latitude": DC_LAT,
            "longitude": DC_LON,
            "hourly": ",".join(HOURLY_VARS),
            "timezone": "GMT",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        },
        timeout=timeout_s,
    )
    resp.raise_for_status()
    return resp.json()


def parse_hourly(payload: dict[str, Any]) -> list[tuple]:
    """Open-Meteo JSON -> rows matching WEATHER_SCHEMA. Hours with no precipitation value are skipped.

    Times come back in GMT as "2026-10-01T14:00"; local date and hour are derived
    with zoneinfo, so daylight saving is handled by the time-zone database.
    """
    hourly = payload["hourly"]

    def col(name: str) -> list:
        return hourly.get(name) or [None] * len(hourly["time"])

    rows = []
    for t, temp, precip, rain, snow, code in zip(
        hourly["time"], *(col(v) for v in HOURLY_VARS), strict=True
    ):
        if precip is None:
            continue
        utc = datetime.fromisoformat(t).replace(tzinfo=UTC)
        local = utc.astimezone(LOCAL_TZ)
        rows.append((
            utc,  # time-zone aware, so Spark stores the right instant whatever the session time zone
            local.date(),
            local.hour,
            None if temp is None else float(temp),
            float(precip),
            None if rain is None else float(rain),
            None if snow is None else float(snow),
            None if code is None else int(code),
            float(precip) >= WET_HOUR_MM,
        ))
    return rows


def fetch_window(today_local: date) -> tuple[date, date]:
    """UTC date range covering yesterday and today in Eastern time."""
    return today_local - timedelta(days=1), today_local + timedelta(days=1)


def upsert_weather(spark: SparkSession, rows: list[tuple], table: str) -> int:
    """MERGE rows into the weather table on hour_utc (idempotent). Returns rows in the batch."""
    from delta.tables import DeltaTable

    df = spark.createDataFrame(rows, WEATHER_SCHEMA)
    if not spark.catalog.tableExists(table):
        df.write.format("delta").mode("append").saveAsTable(table)
    else:
        (
            DeltaTable.forName(spark, table).alias("t")
            .merge(df.alias("s"), "t.hour_utc = s.hour_utc")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    return len(rows)


WEATHER_VIEW_SQL = """
CREATE OR REPLACE VIEW {c}.{s}.gold_route_hour_weather AS
SELECT p.*, w.temperature_c, w.precipitation_mm, w.is_wet
FROM {c}.{s}.gold_route_hour_performance AS p
JOIN {c}.{s}.silver_weather_hourly AS w
  ON w.local_date = p.service_date AND w.local_hour = p.hour_local
"""


def load_weather(
    spark: SparkSession,
    catalog: str = "workspace",
    schema: str = "transit",
    today_local: date | None = None,
) -> dict[str, int]:
    """Fetch recent DC weather, upsert it, and (re)create the weather view once gold exists."""
    import requests

    today_local = today_local or datetime.now(LOCAL_TZ).date()
    start, end = fetch_window(today_local)
    with requests.Session() as session:
        rows = parse_hourly(fetch_hourly(session, start, end))
    if not rows:
        raise RuntimeError(f"Open-Meteo returned no hourly rows for {start}..{end}")
    table = f"{catalog}.{schema}.silver_weather_hourly"
    n = upsert_weather(spark, rows, table)
    if spark.catalog.tableExists(f"{catalog}.{schema}.gold_route_hour_performance"):
        spark.sql(WEATHER_VIEW_SQL.format(c=catalog, s=schema))
    log.info("%s: upserted %d hours (%s..%s UTC)", table, n, start, end)
    return {"hours_upserted": n, "wet_hours": sum(1 for r in rows if r[-1])}
