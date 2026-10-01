"""Gold marts: on-time performance, delay and headways, built from silver.

Runs on Databricks. Everything starts from one fact table:

    silver_vehicle_positions --+
    silver_stop_times ---------+--> gold_timepoint_departures --+--> gold_route_hour_performance
    static_feed_info ----------+    (one row per trip x stop)   +--> gold_headways
                                                                       --> gold_route_hour_bunching

How a departure is observed. A vehicle-position ping says which stop of its trip
the bus is at or heading to (current_stop_sequence). Once that number is past
stop s, the bus has left s. With pings every ~30 s, the departure from s lies
between the last ping still at or before s and the first ping past it:

    ping 12:03:10  seq 7      <- before_ts
                                  (bus leaves stop 7 somewhere in here)
    ping 12:03:40  seq 8      <- after_ts

We take the midpoint, so the error is at most half the gap, and drop
departures whose gap is wider than `max_gap_s` (too uncertain to grade).

On-time window: a departure counts as on time from `early_s` early to
`late_s` late (default 2 min early to 7 min late, the window WMATA uses for
Metrobus; check their current definition before quoting it).

Incremental and idempotent: each run finds the service dates whose silver rows
arrived after the previous build (a high-water mark on _silver_loaded_at, kept
in gold_build_log) and rebuilds that date range in every gold table with
replaceWhere. Rerunning with no new silver rows rebuilds nothing; a late silver
row for an old date rebuilds that date.

Usage in a Databricks notebook:
    from pipelines.gold_marts import build_gold
    build_gold(spark)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

log = logging.getLogger("gold_marts")

LOCAL_TZ = "America/New_York"


@dataclass(frozen=True)
class GoldConfig:
    """Thresholds that define the metrics. Kept in one place so they are easy to state in the README."""

    early_s: int = 120  # up to 2 minutes early still counts as on time
    late_s: int = 420  # up to 7 minutes late still counts as on time
    max_gap_s: int = 120  # widest ping gap around a stop we still grade (error <= half of it)
    bunched_ratio: float = 0.25  # actual headway < 25% of scheduled: buses arrive together
    gapped_ratio: float = 1.5  # actual headway > 150% of scheduled: riders wait much longer


DEFAULT_CONFIG = GoldConfig()


def scheduled_ts(service_date: Column, secs: Column) -> Column:
    """GTFS schedule time (seconds after the service day starts, may exceed 24 h) -> UTC timestamp.

    timestamp_seconds(days * 86400 + secs) gives the local wall-clock time as if it
    were UTC; to_utc_timestamp then shifts it by New York's offset on that date.
    Neither step depends on the Spark session time zone.
    Known limit: GTFS measures from "noon minus 12 h", which differs from midnight
    on the two daylight-saving change days; times before 3 a.m. on those days are off by 1 h.
    """
    from pyspark.sql import functions as F

    return F.to_utc_timestamp(F.timestamp_seconds(F.unix_date(service_date) * 86400 + secs), LOCAL_TZ)


def seconds_between(start: Column, end: Column) -> Column:
    """end - start in whole seconds."""
    return (end.cast("long") - start.cast("long")).cast("int")


def schedule_versions(feed_info: DataFrame) -> DataFrame:
    """Which static release (feed_version) is in force for a service date: [valid_from, valid_to)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.orderBy("valid_from")
    return (
        feed_info.select("feed_version", F.to_date("feed_start_date", "yyyyMMdd").alias("valid_from"))
        .dropDuplicates(["feed_version"])
        .withColumn("valid_to", F.lead("valid_from").over(w))
    )


def schedule_stops(stop_times: DataFrame, timepoints_only: bool | None = None) -> DataFrame:
    """Stops to grade. Timepoints are where agencies measure on-time performance.

    timepoints_only=None picks automatically: timepoints if the schedule marks any, else every stop.
    """
    from pyspark.sql import functions as F

    if timepoints_only is None:
        timepoints_only = stop_times.where(F.col("timepoint") == 1).limit(1).count() > 0
        log.info("grading %s", "timepoints only" if timepoints_only else "every stop (no timepoints marked)")
    if timepoints_only:
        stop_times = stop_times.where(F.col("timepoint") == 1)
    return stop_times.where(F.col("departure_secs").isNotNull()).select(
        "feed_version", "trip_id", "stop_sequence", "stop_id", "departure_secs"
    )


def timepoint_departures(
    vp: DataFrame,
    stops: DataFrame,
    versions: DataFrame,
    cfg: GoldConfig = DEFAULT_CONFIG,
) -> DataFrame:
    """One row per (trip, service_date, stop) whose departure was observed, graded against the schedule."""
    from pyspark.sql import functions as F

    pings = vp.where(F.col("trip_id").isNotNull() & F.col("current_stop_sequence").isNotNull()).select(
        "trip_id", "service_date", "vehicle_id", "route_id", "direction_id", "event_ts",
        F.col("current_stop_sequence").alias("seq"),
    )
    # Point-in-time: each trip instance uses the schedule release in force on its service date.
    trips = (
        pings.select("trip_id", "service_date").distinct().alias("t")
        .join(
            versions.alias("v"),
            (F.col("t.service_date") >= F.col("v.valid_from"))
            & (F.col("v.valid_to").isNull() | (F.col("t.service_date") < F.col("v.valid_to"))),
        )
        .select("t.trip_id", "t.service_date", "v.feed_version")
    )
    planned = trips.join(stops, ["feed_version", "trip_id"])
    ts = F.col("event_ts").cast("long")
    seq, s = F.col("seq"), F.col("stop_sequence")
    bracket = (
        planned.join(pings, ["trip_id", "service_date"])
        .groupBy("trip_id", "service_date", "stop_sequence", "stop_id", "departure_secs", "feed_version")
        .agg(
            F.max(F.when(seq <= s, ts)).alias("before_s"),
            F.min(F.when(seq > s, ts)).alias("after_s"),
            F.max("vehicle_id").alias("vehicle_id"),
            F.max("route_id").alias("route_id"),
            F.max("direction_id").alias("direction_id"),
        )
    )
    gap = F.col("after_s") - F.col("before_s")
    observed = F.timestamp_seconds(((F.col("before_s") + F.col("after_s")) / 2).cast("long"))
    sched = scheduled_ts(F.col("service_date"), F.col("departure_secs"))
    delay = F.col("observed_ts").cast("long") - F.col("scheduled_ts").cast("long")
    return (
        bracket.where((gap > 0) & (gap <= cfg.max_gap_s))
        .select(
            "service_date", "route_id", "direction_id", "trip_id", "vehicle_id", "stop_id", "stop_sequence",
            sched.alias("scheduled_ts"),
            observed.alias("observed_ts"),
            (gap / 2).cast("int").alias("max_error_s"),
            # Scheduled hour of day, straight from GTFS seconds (25:10 -> hour 1), so no time-zone math.
            (F.floor(F.col("departure_secs") / 3600) % 24).cast("int").alias("hour_local"),
            F.date_format("service_date", "E").alias("day_of_week"),
            "feed_version",
        )
        .withColumn("delay_s", delay.cast("int"))
        .withColumn(
            "otp_status",
            F.when(F.col("delay_s") < -cfg.early_s, "early")
            .when(F.col("delay_s") > cfg.late_s, "late")
            .otherwise("on_time"),
        )
    )


def route_hour_performance(departures: DataFrame) -> DataFrame:
    """On-time performance and delay by route, service date and scheduled hour."""
    from pyspark.sql import functions as F

    def n(status: str) -> Column:
        return F.sum(F.when(F.col("otp_status") == status, 1).otherwise(0)).cast("int")

    return (
        departures.groupBy("service_date", "day_of_week", "route_id", "hour_local")
        .agg(
            F.count("*").cast("int").alias("departures"),
            n("on_time").alias("on_time"),
            n("early").alias("early"),
            n("late").alias("late"),
            F.round(F.avg("delay_s"), 1).alias("avg_delay_s"),
            F.percentile_approx("delay_s", 0.5).alias("p50_delay_s"),
            F.percentile_approx("delay_s", 0.9).alias("p90_delay_s"),
        )
        .withColumn("pct_on_time", F.round(100.0 * F.col("on_time") / F.col("departures"), 1))
    )


def headways(departures: DataFrame, cfg: GoldConfig = DEFAULT_CONFIG) -> DataFrame:
    """Time between consecutive observed buses of a route and direction at the same stop.

    Each headway is compared with the scheduled gap between the *same two trips*.
    If a bus in between was not observed, both gaps span it, so the ratio stays fair.
    A negative scheduled gap means the later-scheduled bus left first (it overtook): "out_of_order".
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("service_date", "route_id", "direction_id", "stop_id").orderBy(
        "observed_ts", "trip_id"
    )
    prev_obs, prev_sched = F.lag("observed_ts").over(w), F.lag("scheduled_ts").over(w)
    out = (
        departures.select(
            "service_date", "day_of_week", "route_id", "direction_id", "stop_id", "hour_local", "trip_id",
            "observed_ts", "scheduled_ts",
            F.lag("trip_id").over(w).alias("prev_trip_id"),
            seconds_between(prev_obs, F.col("observed_ts")).alias("headway_s"),
            seconds_between(prev_sched, F.col("scheduled_ts")).alias("sched_headway_s"),
        )
        .where(F.col("prev_trip_id").isNotNull())
    )
    ratio = F.col("headway_s") / F.col("sched_headway_s")
    return out.withColumn(
        "headway_ratio", F.when(F.col("sched_headway_s") > 0, F.round(ratio, 3))
    ).withColumn(
        "headway_status",
        F.when(F.col("sched_headway_s") <= 0, "out_of_order")
        .when(F.col("headway_ratio") < cfg.bunched_ratio, "bunched")
        .when(F.col("headway_ratio") > cfg.gapped_ratio, "gapped")
        .otherwise("regular"),
    )


def route_hour_bunching(hw: DataFrame) -> DataFrame:
    """Share of bunched and gapped headways by route, service date and scheduled hour."""
    from pyspark.sql import functions as F

    def n(status: str) -> Column:
        return F.sum(F.when(F.col("headway_status") == status, 1).otherwise(0)).cast("int")

    return (
        hw.groupBy("service_date", "day_of_week", "route_id", "hour_local")
        .agg(
            F.count("*").cast("int").alias("headways"),
            n("bunched").alias("bunched"),
            n("gapped").alias("gapped"),
            n("out_of_order").alias("out_of_order"),
            F.percentile_approx("headway_ratio", 0.5).alias("p50_headway_ratio"),
        )
        .withColumn("pct_bunched", F.round(100.0 * F.col("bunched") / F.col("headways"), 1))
    )


# --------------------------------------------------------------------------- build


GOLD_TABLES = ("gold_timepoint_departures", "gold_route_hour_performance", "gold_headways",
               "gold_route_hour_bunching")


def dates_to_rebuild(
    spark: SparkSession, vp: DataFrame, build_log: str, full_refresh: bool = False
) -> tuple[list[date], object]:
    """Service dates with silver rows newer than the last build, and the new high-water mark.

    The mark is the newest _silver_loaded_at seen *now*, not the wall clock, so a
    silver row that lands while gold is building is picked up next run, not skipped.
    """
    from pyspark.sql import functions as F

    mark = vp.agg(F.max("_silver_loaded_at")).first()[0]
    new = vp
    if not full_refresh and spark.catalog.tableExists(build_log):
        last = spark.table(build_log).agg(F.max("silver_loaded_through")).first()[0]
        if last is not None:
            new = vp.where(F.col("_silver_loaded_at") > F.lit(last))
    dates = sorted(r[0] for r in new.select("service_date").distinct().collect() if r[0] is not None)
    return dates, mark


def write_range(df: DataFrame, table: str, start: date, end: date) -> None:
    """Replace one service-date range of a gold table (idempotent: rerunning gives the same rows)."""
    spark = df.sparkSession
    writer = df.write.format("delta")
    if spark.catalog.tableExists(table):
        cond = f"service_date >= DATE'{start}' AND service_date <= DATE'{end}'"
        writer.mode("overwrite").option("replaceWhere", cond).saveAsTable(table)
    else:
        writer.mode("append").saveAsTable(table)


def build_gold(
    spark: SparkSession,
    catalog: str = "workspace",
    schema: str = "transit",
    full_refresh: bool = False,
    cfg: GoldConfig = DEFAULT_CONFIG,
    timepoints_only: bool | None = None,
) -> dict[str, object]:
    """Rebuild the gold tables for every service date that received new silver rows.

    full_refresh=True rebuilds every date (use after changing GoldConfig or loading a new schedule).
    Returns the date range and rows written per table.
    """
    from pyspark.sql import functions as F

    def t(name: str) -> str:
        return f"{catalog}.{schema}.{name}"

    vp = spark.table(t("silver_vehicle_positions"))
    build_log = t("gold_build_log")
    dates, mark = dates_to_rebuild(spark, vp, build_log, full_refresh)
    if not dates:
        log.info("gold: no new silver rows since the last build")
        return {"start": None, "end": None, "rows": {}}
    start, end = dates[0], dates[-1]
    in_range = F.col("service_date").between(F.lit(start), F.lit(end))

    stops = schedule_stops(spark.table(t("silver_stop_times")), timepoints_only)
    versions = schedule_versions(spark.table(t("static_feed_info")))
    deps = timepoint_departures(vp.where(in_range), stops, versions, cfg).withColumn(
        "_gold_built_at", F.current_timestamp()
    )
    write_range(deps, t("gold_timepoint_departures"), start, end)

    # Downstream marts read the table just written, so the expensive join runs once.
    deps = spark.table(t("gold_timepoint_departures")).where(in_range)
    write_range(route_hour_performance(deps), t("gold_route_hour_performance"), start, end)
    write_range(headways(deps, cfg), t("gold_headways"), start, end)
    hw = spark.table(t("gold_headways")).where(in_range)
    write_range(route_hour_bunching(hw), t("gold_route_hour_bunching"), start, end)

    rows = {name: spark.table(t(name)).where(in_range).count() for name in GOLD_TABLES}
    # Advance the high-water mark only after every table is written: a failed run is simply redone.
    entry = spark.createDataFrame(
        [(start, end, mark)], "start_date date, end_date date, silver_loaded_through timestamp"
    ).withColumn("built_at", F.current_timestamp())
    entry.write.format("delta").mode("append").saveAsTable(build_log)
    log.info("gold %s..%s: %s", start, end, rows)
    return {"start": start, "end": end, "rows": rows}


def gold_freshness_s(spark: SparkSession, catalog: str = "workspace", schema: str = "transit") -> int | None:
    """Seconds between now and the newest observed departure in gold (the brief's freshness metric)."""
    from pyspark.sql import functions as F

    table = f"{catalog}.{schema}.gold_timepoint_departures"
    if not spark.catalog.tableExists(table):
        return None
    return spark.table(table).agg(
        (F.unix_timestamp(F.current_timestamp()) - F.max(F.col("observed_ts")).cast("long")).cast("int")
    ).first()[0]
