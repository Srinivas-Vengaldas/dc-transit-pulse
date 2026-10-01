"""Typed silver copy of the GTFS static stop_times table.

GTFS times are "seconds after the start of the service day" written as
H:MM:SS, and the hour can be 24 or more: a trip leaving at 00:30 the next
morning is written 24:30:00 on the previous service day. Two traps follow:

- They are not clock times, so casting them to a timestamp fails or is wrong.
- Compared as text they sort wrongly: "7:00:00" > "24:00:00" because "7" > "2".

So silver stores them as integer seconds (arrival_secs, departure_secs). Gold
turns them into real timestamps per service day when it needs to.

Usage in a Databricks notebook:
    from pipelines.static_silver import build_silver_stop_times
    build_silver_stop_times(spark, feed_version="S1000251")
"""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

log = logging.getLogger("static_silver")

GTFS_TIME_RE = r"^\s*(\d{1,3}):([0-5]\d):([0-5]\d)\s*$"


def gtfs_time_to_seconds(value: str | None) -> int | None:
    """Plain-Python version: "25:10:00" -> 90600. None for blank or malformed values."""
    if value is None:
        return None
    m = re.match(GTFS_TIME_RE, value)
    if not m:
        return None
    h, mi, s = (int(g) for g in m.groups())
    return h * 3600 + mi * 60 + s


def gtfs_time_seconds_col(col: Column) -> Column:
    """Spark version of gtfs_time_to_seconds. Built-in functions only, so no Python UDF overhead."""
    from pyspark.sql import functions as F

    def part(i: int) -> Column:
        return F.regexp_extract(col, GTFS_TIME_RE, i).cast("int")

    return F.when(col.rlike(GTFS_TIME_RE), part(1) * 3600 + part(2) * 60 + part(3))


def transform_stop_times(static_stop_times: DataFrame) -> DataFrame:
    """All-string static_stop_times -> typed columns."""
    from pyspark.sql import functions as F

    cols = set(static_stop_times.columns)

    def opt(name: str, cast: str) -> Column:
        return (F.col(name).cast(cast) if name in cols else F.lit(None).cast(cast)).alias(name)

    return static_stop_times.select(
        F.col("feed_version"),
        F.col("trip_id"),
        F.col("stop_id"),
        F.col("stop_sequence").cast("int").alias("stop_sequence"),
        gtfs_time_seconds_col(F.col("arrival_time")).alias("arrival_secs"),
        gtfs_time_seconds_col(F.col("departure_time")).alias("departure_secs"),
        opt("timepoint", "int"),
        opt("shape_dist_traveled", "double"),
    )


def build_silver_stop_times(
    spark: SparkSession, feed_version: str, catalog: str = "workspace", schema: str = "transit"
) -> int:
    """Write one feed_version of typed stop_times to silver_stop_times (replaceWhere, so reruns are safe)."""
    from pyspark.sql import functions as F

    if not re.fullmatch(r"[\w.-]+", feed_version):
        raise ValueError(f"unexpected feed_version {feed_version!r}")
    src = spark.table(f"{catalog}.{schema}.static_stop_times").where(F.col("feed_version") == feed_version)
    df = transform_stop_times(src)
    table = f"{catalog}.{schema}.silver_stop_times"
    writer = df.write.format("delta")
    if spark.catalog.tableExists(table):
        writer.mode("overwrite").option("replaceWhere", f"feed_version = '{feed_version}'").saveAsTable(table)
    else:
        writer.mode("append").saveAsTable(table)
    n = spark.table(table).where(F.col("feed_version") == feed_version).count()
    log.info("%s feed_version=%s: %d rows", table, feed_version, n)
    return n
