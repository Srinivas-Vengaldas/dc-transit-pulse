"""Build typed, deduplicated silver tables from the bronze GTFS-RT tables.

Runs on Databricks. For each feed there are two availableNow streams that read
the bronze Delta table incrementally (each with its own checkpoint):

    bronze_<feed> --parse/cast--> valid rows   --watermark dedupe--> MERGE --> silver_<feed>
                               \\-> invalid rows --append--------------------> silver_quarantine

Why two streams instead of one: the dedupe step needs a non-null event time for
its watermark, so bad rows have to be split off *before* it. Two queries over
the same bronze table are cheap and each stays simple.

Dedupe happens twice on purpose:
1. dropDuplicatesWithinWatermark: drops repeat pings inside the stream. The
   watermark (max event time seen minus `watermark_delay`) bounds how long a key
   is remembered, so streaming state does not grow forever. Rows whose event
   time is already behind the watermark are dropped as late.
2. MERGE ... WHEN NOT MATCHED THEN INSERT on the key: makes the table write
   idempotent. If a checkpoint is lost and bronze is replayed, nothing is
   inserted twice.

Usage in a Databricks notebook:
    import sys
    sys.path.append("/Workspace/Users/<you>/dc-transit-pulse")
    from pipelines.silver_transform import run_silver
    run_silver(spark, raw_volume="/Volumes/workspace/transit/raw")
"""
from __future__ import annotations

import logging
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import DataType

log = logging.getLogger("silver_transform")

LOCAL_TZ = "America/New_York"
DEFAULT_WATERMARK = "30 minutes"
# A ping stamped this far after we fetched it is a clock error, not data.
MAX_FUTURE_SKEW_S = 300


# --------------------------------------------------------------------------- helpers


def field_or_null(schema: DataType, path: str) -> Column:
    """Column for a dotted path such as "entity.vehicle.position.speed", or NULL if it is absent.

    Auto Loader infers bronze's nested schema from the data seen so far, so an
    optional GTFS-RT field (bearing, occupancy_status, ...) that WMATA has never
    sent does not exist yet. Referencing it directly would fail the whole job;
    this returns a typed NULL instead, and the field fills in once it appears.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    node = schema
    for part in path.split("."):
        if not isinstance(node, StructType) or part not in node.fieldNames():
            return F.lit(None).cast("string")
        node = node[part].dataType
    return F.col(path)


def epoch_to_ts(col: Column) -> Column:
    """GTFS-RT epoch seconds (a string in bronze) -> timestamp; NULL if not a number."""
    from pyspark.sql import functions as F

    secs = col.cast("string")
    return F.when(secs.rlike(r"^\d+$"), F.timestamp_seconds(secs.cast("long")))


def gtfs_date(col: Column) -> Column:
    """GTFS date "20260928" -> date; NULL if malformed."""
    from pyspark.sql import functions as F

    s = col.cast("string")
    return F.when(s.rlike(r"^\d{8}$"), F.to_date(s, "yyyyMMdd"))


def to_int(col: Column) -> Column:
    """String -> int; NULL (not an error) when the value is not an integer."""
    from pyspark.sql import functions as F

    s = col.cast("string")
    return F.when(s.rlike(r"^-?\d+$"), s.cast("int"))


def to_double(col: Column) -> Column:
    """String -> double; NULL (not an error) when the value is not a number."""
    from pyspark.sql import functions as F

    s = col.cast("string")
    return F.when(s.rlike(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$"), s.cast("double"))


def service_date(trip_start_date: Column, event_ts: Column) -> Column:
    """The transit service day a row belongs to.

    Prefer the trip's start_date: a trip that starts at 23:50 and runs past
    midnight belongs to the previous service day. If the vehicle is not on a
    trip, fall back to the local (Eastern) calendar date of the event.
    """
    from pyspark.sql import functions as F

    return F.coalesce(trip_start_date, F.to_date(F.from_utc_timestamp(event_ts, LOCAL_TZ)))


# --------------------------------------------------------------------------- parsing

# GTFS-RT entity shapes as bronze holds them (protobuf -> JSON, every leaf kept as a string).
# Used when Auto Loader stored `entity` as a JSON string instead of a struct.
_TRIP = ("struct<trip_id:string,route_id:string,direction_id:string,start_time:string,"
         "start_date:string,schedule_relationship:string>")
_VEHICLE_DESC = "struct<id:string,label:string,license_plate:string>"
_STE = "struct<delay:string,time:string,uncertainty:string>"
VP_ENTITY_SCHEMA = (
    f"struct<id:string,vehicle:struct<trip:{_TRIP},vehicle:{_VEHICLE_DESC},"
    "position:struct<latitude:string,longitude:string,bearing:string,odometer:string,speed:string>,"
    "current_stop_sequence:string,stop_id:string,current_status:string,timestamp:string,"
    "congestion_level:string,occupancy_status:string>>"
)
TU_ENTITY_SCHEMA = (
    f"struct<id:string,trip_update:struct<trip:{_TRIP},vehicle:{_VEHICLE_DESC},"
    f"stop_time_update:array<struct<stop_sequence:string,stop_id:string,arrival:{_STE},"
    f"departure:{_STE},schedule_relationship:string>>,timestamp:string,delay:string>>"
)


def with_entity_struct(bronze: DataFrame, entity_schema: str) -> DataFrame:
    """Make sure `entity` is a struct.

    With inferColumnTypes=false, Auto Loader can store a nested JSON object as
    one JSON string. Parsing it here with an explicit schema means silver works
    for either bronze shape; numbers in the JSON are read into the string fields
    as their text, so nothing is lost before the casts below.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    if isinstance(bronze.schema["entity"].dataType, StringType):
        return bronze.withColumn("entity", F.from_json("entity", entity_schema))
    return bronze



def parse_vehicle_positions(bronze: DataFrame) -> DataFrame:
    """Flatten and type bronze vehicle positions. Adds `_dq_reason` (NULL = valid row)."""
    from pyspark.sql import functions as F

    bronze = with_entity_struct(bronze, VP_ENTITY_SCHEMA)
    s = bronze.schema

    def f(path: str) -> Column:
        return field_or_null(s, f"entity.vehicle.{path}")

    fetched_at = F.to_timestamp(F.col("fetched_at"))
    event_ts = epoch_to_ts(f("timestamp"))
    trip_start_date = gtfs_date(f("trip.start_date"))
    df = bronze.select(
        f("vehicle.id").alias("vehicle_id"),
        event_ts.alias("event_ts"),
        service_date(trip_start_date, event_ts).alias("service_date"),
        f("trip.route_id").alias("route_id"),
        f("trip.trip_id").alias("trip_id"),
        to_int(f("trip.direction_id")).alias("direction_id"),
        trip_start_date.alias("trip_start_date"),
        f("trip.start_time").alias("trip_start_time"),
        f("vehicle.label").alias("vehicle_label"),
        f("current_status").alias("current_status"),
        f("stop_id").alias("stop_id"),
        to_int(f("current_stop_sequence")).alias("current_stop_sequence"),
        to_double(f("position.latitude")).alias("latitude"),
        to_double(f("position.longitude")).alias("longitude"),
        to_double(f("position.bearing")).alias("bearing"),
        to_double(f("position.speed")).alias("speed_mps"),
        f("occupancy_status").alias("occupancy_status"),
        (fetched_at.cast("long") - event_ts.cast("long")).alias("staleness_s"),
        epoch_to_ts(F.col("feed_header_ts")).alias("feed_header_ts"),
        fetched_at.alias("fetched_at"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )
    lat, lon = F.col("latitude"), F.col("longitude")
    reason = (
        F.when(F.col("vehicle_id").isNull() | (F.trim(F.col("vehicle_id")) == ""), "missing_vehicle_id")
        .when(F.col("event_ts").isNull(), "missing_or_bad_timestamp")
        .when(lat.isNull() | lon.isNull(), "missing_position")
        .when((F.abs(lat) > 90) | (F.abs(lon) > 180) | ((lat == 0) & (lon == 0)), "invalid_position")
        .when(F.col("staleness_s") < -MAX_FUTURE_SKEW_S, "timestamp_in_future")
    )
    return df.withColumn("_dq_reason", reason)


def parse_trip_updates(bronze: DataFrame) -> DataFrame:
    """Flatten and type bronze trip updates at trip level. Adds `_dq_reason` (NULL = valid row).

    stop_time_update holds predictions for the stops still ahead; only its size is
    kept here. Trip-level `delay` is what the gold delay-by-route mart needs.
    If WMATA omits the per-trip timestamp, the snapshot header time is used.
    """
    from pyspark.sql import functions as F

    bronze = with_entity_struct(bronze, TU_ENTITY_SCHEMA)
    s = bronze.schema

    def f(path: str) -> Column:
        return field_or_null(s, f"entity.trip_update.{path}")

    fetched_at = F.to_timestamp(F.col("fetched_at"))
    header_ts = epoch_to_ts(F.col("feed_header_ts"))
    event_ts = F.coalesce(epoch_to_ts(f("timestamp")), header_ts)
    trip_start_date = gtfs_date(f("trip.start_date"))
    stus = f("stop_time_update")
    n_stus = F.size(stus) if _is_array(s, "entity.trip_update.stop_time_update") else F.lit(None)
    df = bronze.select(
        f("trip.trip_id").alias("trip_id"),
        event_ts.alias("event_ts"),
        service_date(trip_start_date, event_ts).alias("service_date"),
        f("trip.route_id").alias("route_id"),
        to_int(f("trip.direction_id")).alias("direction_id"),
        trip_start_date.alias("trip_start_date"),
        f("trip.start_time").alias("trip_start_time"),
        f("trip.schedule_relationship").alias("schedule_relationship"),
        f("vehicle.id").alias("vehicle_id"),
        to_int(f("delay")).alias("delay_s"),
        n_stus.cast("int").alias("n_stop_time_updates"),
        header_ts.alias("feed_header_ts"),
        fetched_at.alias("fetched_at"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )
    reason = (
        F.when(F.col("trip_id").isNull() | (F.trim(F.col("trip_id")) == ""), "missing_trip_id")
        .when(F.col("event_ts").isNull(), "missing_or_bad_timestamp")
        .when(F.col("fetched_at").cast("long") - F.col("event_ts").cast("long") < -MAX_FUTURE_SKEW_S,
              "timestamp_in_future")
    )
    return df.withColumn("_dq_reason", reason)


def _is_array(schema: DataType, path: str) -> bool:
    from pyspark.sql.types import ArrayType, StructType

    node = schema
    for part in path.split("."):
        if not isinstance(node, StructType) or part not in node.fieldNames():
            return False
        node = node[part].dataType
    return isinstance(node, ArrayType)


# --------------------------------------------------------------------------- feeds


@dataclass(frozen=True)
class SilverFeed:
    """How one bronze feed becomes a silver table."""

    name: str
    parse: Callable[[DataFrame], DataFrame]
    key: tuple[str, ...]

    def bronze_table(self, catalog: str, schema: str) -> str:
        return f"{catalog}.{schema}.bronze_{self.name}"

    def silver_table(self, catalog: str, schema: str) -> str:
        return f"{catalog}.{schema}.silver_{self.name}"


FEEDS: dict[str, SilverFeed] = {
    "vehicle_positions": SilverFeed("vehicle_positions", parse_vehicle_positions, ("vehicle_id", "event_ts")),
    # trip_start_date is in the key: the same trip_id runs again on another service day.
    "trip_updates": SilverFeed(
        "trip_updates", parse_trip_updates, ("trip_id", "trip_start_date", "event_ts")
    ),
}


def checkpoint_paths(raw_volume: str, feed: str) -> dict[str, str]:
    """Checkpoint locations for a feed's silver and quarantine streams, inside the raw Volume."""
    root = raw_volume.rstrip("/")
    return {
        "silver": f"{root}/_checkpoints/silver_{feed}",
        "quarantine": f"{root}/_checkpoints/silver_quarantine_{feed}",
    }


def merge_condition(key: tuple[str, ...]) -> str:
    """MERGE match condition on the key. `<=>` is null-safe equality (NULL keys still match)."""
    return " AND ".join(f"t.{k} <=> s.{k}" for k in key)


def ensure_table(spark: SparkSession, table: str, like: DataFrame) -> bool:
    """Create an empty Delta table with `like`'s schema if it does not exist yet. True if created."""
    if spark.catalog.tableExists(table):
        return False
    like.limit(0).write.format("delta").mode("append").saveAsTable(table)
    log.info("created %s", table)
    return True


def reset_silver(
    spark: SparkSession, raw_volume: str, catalog: str = "workspace", schema: str = "transit"
) -> None:
    """Drop the silver tables AND their checkpoints, so the next run_silver rebuilds from all of bronze.

    A table and its checkpoint are one unit: the checkpoint records which bronze
    rows were already written to the table. Dropping only the table leaves a
    checkpoint that says "done", and the rebuilt table stays empty.
    """
    for feed, spec in FEEDS.items():
        spark.sql(f"DROP TABLE IF EXISTS {spec.silver_table(catalog, schema)}")
        for path in checkpoint_paths(raw_volume, feed).values():
            shutil.rmtree(path, ignore_errors=True)
            if os.path.exists(path):
                raise RuntimeError(f"could not delete checkpoint {path}")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.silver_quarantine")
    log.info("silver tables and checkpoints removed")


def make_merge_batch(table: str, key: tuple[str, ...]) -> Callable[[DataFrame, int], None]:
    """foreachBatch function that inserts only rows whose key is not in `table` yet.

    The batch's minimum event time is added to the condition so Delta can skip
    files whose event_ts range is entirely older (data skipping): the MERGE does
    not have to scan the whole history to find the few keys it might match.
    """
    condition = merge_condition(key)

    def merge_batch(batch: DataFrame, batch_id: int) -> None:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        batch = batch.dropDuplicates(list(key))
        # Epoch seconds, not a datetime: avoids any time-zone conversion on the way back.
        min_epoch = batch.agg(F.min(F.col("event_ts").cast("long"))).first()[0]
        if min_epoch is None:
            return
        cond = f"{condition} AND t.event_ts >= timestamp_seconds({int(min_epoch)})"
        (
            DeltaTable.forName(batch.sparkSession, table)
            .alias("t")
            .merge(batch.alias("s"), cond)
            .whenNotMatchedInsertAll()
            .execute()
        )

    return merge_batch


def _rows_read(query: object) -> int:
    """Sum of numInputRows over a finished query's progress (dict on classic Spark, object on Connect)."""
    total = 0
    for p in query.recentProgress:  # type: ignore[attr-defined]
        try:
            total += int(p["numInputRows"])
        except (TypeError, KeyError):
            total += int(getattr(p, "numInputRows", 0))
    return total


def run_silver_feed(
    spark: SparkSession,
    feed: str,
    raw_volume: str,
    catalog: str = "workspace",
    schema: str = "transit",
    watermark_delay: str = DEFAULT_WATERMARK,
) -> dict[str, int]:
    """Process new bronze rows of one feed into silver and quarantine.

    Returns bronze rows read and rows added to silver and quarantine. Rows read
    minus the two "added" counts is what dedupe and the watermark removed.
    """
    if feed not in FEEDS:
        raise ValueError(f"unknown feed {feed!r}; expected one of {tuple(FEEDS)}")
    from pyspark.sql import functions as F

    spec = FEEDS[feed]
    cps = checkpoint_paths(raw_volume, feed)
    silver = spec.silver_table(catalog, schema)
    quarantine = f"{catalog}.{schema}.silver_quarantine"

    def quarantined() -> int:
        if not spark.catalog.tableExists(quarantine):
            return 0
        return spark.table(quarantine).where(F.col("feed") == feed).count()

    parsed = spec.parse(spark.readStream.table(spec.bronze_table(catalog, schema)))
    valid = (
        parsed.where(F.col("_dq_reason").isNull())
        .drop("_dq_reason")
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )
    # Same columns as `valid`, from a batch read, so the MERGE target exists before the first batch.
    created = ensure_table(
        spark,
        silver,
        spec.parse(spark.table(spec.bronze_table(catalog, schema)))
        .drop("_dq_reason")
        .withColumn("_silver_loaded_at", F.current_timestamp()),
    )
    if created and os.path.exists(cps["silver"]):
        raise RuntimeError(
            f"{silver} was missing but its checkpoint {cps['silver']} exists, so the new table would "
            "silently skip every bronze row already processed. Run reset_silver() to start clean."
        )
    silver_before, quarantine_before = spark.table(silver).count(), quarantined()
    silver_q = (
        valid.withWatermark("event_ts", watermark_delay)
        .dropDuplicatesWithinWatermark(list(spec.key))
        .writeStream.foreachBatch(make_merge_batch(silver, spec.key))
        .option("checkpointLocation", cps["silver"])
        .trigger(availableNow=True)
        .start()
    )
    silver_q.awaitTermination()

    # Quarantine keeps the whole parsed row as JSON so any feed fits one table.
    bad = parsed.where(F.col("_dq_reason").isNotNull()).select(
        F.lit(feed).alias("feed"),
        F.col("_dq_reason").alias("reason"),
        F.to_json(F.struct(*[c for c in parsed.columns if c != "_dq_reason"])).alias("record"),
        F.col("_source_file"),
        F.current_timestamp().alias("_quarantined_at"),
    )
    quarantine_q = (
        bad.writeStream.format("delta")
        .option("checkpointLocation", cps["quarantine"])
        .trigger(availableNow=True)
        .toTable(quarantine)
    )
    quarantine_q.awaitTermination()

    counts = {
        "bronze_rows_read": _rows_read(silver_q),
        "silver_rows_added": spark.table(silver).count() - silver_before,
        "quarantine_rows_added": quarantined() - quarantine_before,
    }
    log.info("%s: %s", feed, counts)
    return counts


def run_silver(
    spark: SparkSession,
    raw_volume: str,
    catalog: str = "workspace",
    schema: str = "transit",
    watermark_delay: str = DEFAULT_WATERMARK,
) -> dict[str, dict[str, int]]:
    """Run every feed's silver step."""
    return {f: run_silver_feed(spark, f, raw_volume, catalog, schema, watermark_delay) for f in FEEDS}


def reconcile(spark: SparkSession, feed: str, catalog: str = "workspace", schema: str = "transit") -> dict:
    """Account for every bronze row of a feed: bronze = quarantined + duplicates + late + silver.

    Re-parses bronze in batch with the same rules the stream uses, so the split
    is exact. Run it right after run_silver, when silver has seen all of bronze.
    This is the source of the "duplicates removed" metric.
    """
    from pyspark.sql import functions as F

    spec = FEEDS[feed]
    parsed = spec.parse(spark.table(spec.bronze_table(catalog, schema)))
    valid = parsed.where(F.col("_dq_reason").isNull())
    bronze_rows = parsed.count()
    valid_rows = valid.count()
    distinct_keys = valid.select(*spec.key).distinct().count()
    silver_rows = spark.table(spec.silver_table(catalog, schema)).count()
    duplicates = valid_rows - distinct_keys
    out = {
        "bronze_rows": bronze_rows,
        "quarantined": bronze_rows - valid_rows,
        "duplicates": duplicates,
        "dropped_late": distinct_keys - silver_rows,
        "silver_rows": silver_rows,
        "pct_duplicates": round(100.0 * duplicates / bronze_rows, 2) if bronze_rows else 0.0,
    }
    log.info("%s reconciliation: %s", feed, out)
    return out
