"""Incrementally ingest landed GTFS-RT JSON Lines files into bronze Delta tables with Auto Loader.

Runs on Databricks. One stream per feed:

    /Volumes/.../landing/vehicle_positions/**.jsonl -> <catalog>.<schema>.bronze_vehicle_positions
    /Volumes/.../landing/trip_updates/**.jsonl      -> <catalog>.<schema>.bronze_trip_updates

It uses trigger(availableNow=True): each run processes every file that arrived
since the last run, then stops. That gives streaming's exactly-once file
tracking (the checkpoint) with batch-style cost: compute runs only while there
is work, which matters on the free tier.

Usage in a Databricks notebook:
    import sys
    sys.path.append("/Workspace/Users/<you>/dc-transit-pulse")
    from pipelines.bronze_ingest import ingest_all
    ingest_all(spark, raw_volume="/Volumes/workspace/transit/raw")
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("bronze_ingest")

FEEDS = ("vehicle_positions", "trip_updates")


def autoloader_options(schema_location: str) -> dict[str, str]:
    """Auto Loader settings for the landing files.

    - format json: each .jsonl line is one record.
    - schemaLocation: where Auto Loader stores the inferred schema, so it is not
      re-inferred every run and changes are tracked over time.
    - addNewColumns: if WMATA adds a field, the stream stops once, records the new
      column, and continues on restart (schema evolution).
    - inferColumnTypes false: nested fields stay strings in bronze; silver casts them.
      Values that do not match the schema land in _rescued_data instead of being lost.
    - partitionColumns ingest_date: read from the folder name ingest_date=YYYY-MM-DD.
    """
    return {
        "cloudFiles.format": "json",
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.inferColumnTypes": "false",
        "cloudFiles.partitionColumns": "ingest_date",
        "pathGlobFilter": "*.jsonl",
    }


def paths_for(raw_volume: str, feed: str) -> dict[str, str]:
    """Landing, schema and checkpoint locations for one feed, all inside the raw Volume."""
    root = raw_volume.rstrip("/")
    return {
        "source": f"{root}/landing/{feed}",
        "schema": f"{root}/_autoloader/{feed}/schema",
        "checkpoint": f"{root}/_autoloader/{feed}/checkpoint",
    }


def _input_rows(progress: object) -> int:
    """numInputRows from a progress entry (a dict on classic Spark, an object on Spark Connect)."""
    try:
        return int(progress["numInputRows"])  # type: ignore[index]
    except (TypeError, KeyError):
        return int(getattr(progress, "numInputRows", 0))


def ingest_feed(
    spark: SparkSession, feed: str, raw_volume: str, catalog: str = "workspace", schema: str = "transit"
) -> int:
    """Run one availableNow Auto Loader batch for a feed. Returns rows added to bronze."""
    if feed not in FEEDS:
        raise ValueError(f"unknown feed {feed!r}; expected one of {FEEDS}")
    from pyspark.sql import functions as F
    p = paths_for(raw_volume, feed)
    table = f"{catalog}.{schema}.bronze_{feed}"

    stream = (
        spark.readStream.format("cloudFiles")
        .options(**autoloader_options(p["schema"]))
        .load(p["source"])
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    query = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", p["checkpoint"])
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.awaitTermination()

    added = sum(_input_rows(prog) for prog in query.recentProgress)
    log.info("%s: %d new rows", table, added)
    return added


def ingest_all(
    spark: SparkSession, raw_volume: str, catalog: str = "workspace", schema: str = "transit"
) -> dict[str, int]:
    """Ingest every feed; returns rows added per bronze table."""
    return {feed: ingest_feed(spark, feed, raw_volume, catalog, schema) for feed in FEEDS}
