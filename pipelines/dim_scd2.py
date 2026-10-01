"""SCD Type 2 dimensions for GTFS routes and stops, built from the static tables.

Each GTFS static release (feed_version) is a full snapshot of routes and stops.
Applying a release to a dimension keeps history instead of overwriting it:

    route_id  route_long_name   valid_from  valid_to    is_current
    A1        Old Name          2026-09-13  2027-03-28  false      <- closed when the name changed
    A1        New Name          2027-03-28  NULL        true       <- new version
    Z9        Gone Route        2026-09-13  2027-03-28  false      <- closed: not in the new release

valid_from is the release's feed_start_date; valid_to is exclusive. A fact row
looks up the version that was in force on its service_date (a point-in-time
join, see ENRICHED_VP_VIEW_SQL), so history is reported with the names that
were true at the time.

Applying the same release twice changes nothing (idempotent): unchanged rows
match on attr_hash and are left alone.

Usage in a Databricks notebook:
    from pipelines.dim_scd2 import load_dims, create_enriched_view
    load_dims(spark, feed_version="S1000251")
    create_enriched_view(spark)
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

log = logging.getLogger("dim_scd2")


@dataclass(frozen=True)
class DimSpec:
    """One SCD2 dimension: its static source table, business key and the columns whose changes are tracked."""

    name: str
    source: str
    key: str
    tracked: tuple[str, ...]


DIMS: dict[str, DimSpec] = {
    "routes": DimSpec(
        "routes",
        "static_routes",
        "route_id",
        ("agency_id", "route_short_name", "route_long_name", "route_desc", "route_type",
         "route_url", "route_color", "route_text_color"),
    ),
    "stops": DimSpec(
        "stops",
        "static_stops",
        "stop_id",
        ("stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id",
         "location_type", "parent_station", "wheelchair_boarding"),
    ),
}


def attr_hash(cols: list[Column]) -> Column:
    """SHA-256 of the tracked values, used to detect a change in one comparison.

    NULLs become "\\u0000" first: concat_ws skips NULLs, so without this
    ("a", NULL) and (NULL, "a") would hash the same.
    """
    from pyspark.sql import functions as F

    return F.sha2(F.concat_ws("\u001f", *[F.coalesce(c.cast("string"), F.lit("\u0000")) for c in cols]), 256)


def prepare_source(source: DataFrame, spec: DimSpec, feed_version: str, valid_from: date) -> DataFrame:
    """One release's rows shaped like the dimension (without valid_to / is_current)."""
    from pyspark.sql import functions as F

    cols = set(source.columns)
    tracked = [(F.col(c) if c in cols else F.lit(None).cast("string")).alias(c) for c in spec.tracked]
    return (
        source.select(F.col(spec.key), *tracked)
        .dropDuplicates([spec.key])
        .withColumn("attr_hash", attr_hash([F.col(c) for c in spec.tracked]))
        .withColumn("feed_version", F.lit(feed_version))
        .withColumn("valid_from", F.lit(valid_from).cast("date"))
    )


def apply_scd2(spark: SparkSession, src: DataFrame, table: str, spec: DimSpec) -> dict[str, int]:
    """Apply one prepared release to the dimension table with a single MERGE.

    The "staged updates" trick: every source row is staged once with
    merge_key = key (to match and close the current version), and changed rows
    are staged a second time with merge_key = NULL, which never matches, so the
    new version is inserted in the same atomic MERGE.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    sk = f"{spec.name.removesuffix('s')}_sk"
    valid_from = src.select("valid_from").first()
    if valid_from is None:
        raise ValueError(f"{table}: source release is empty")
    valid_from = valid_from[0]

    if not spark.catalog.tableExists(table):
        (
            src.withColumn(sk, F.xxhash64(spec.key, "valid_from"))
            .withColumn("valid_to", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta").mode("append").saveAsTable(table)
        )
        n = spark.table(table).count()
        log.info("%s created with %d rows", table, n)
        return {"inserted": n, "closed": 0}

    target = spark.table(table)
    latest = target.where("is_current").agg(F.max("valid_from")).first()[0]
    if latest is not None and valid_from < latest:
        raise ValueError(f"{table}: release valid_from {valid_from} is older than current {latest}; "
                         "apply releases in order")

    current = target.where("is_current").select(spec.key, F.col("attr_hash").alias("_cur_hash"))
    changed = src.join(current, spec.key).where(F.col("attr_hash") != F.col("_cur_hash")).drop("_cur_hash")
    staged = src.withColumn("merge_key", F.col(spec.key)).unionByName(
        changed.withColumn("merge_key", F.lit(None).cast("string"))
    )
    copied = [spec.key, *spec.tracked, "attr_hash", "feed_version", "valid_from"]
    insert_values = {c: f"s.{c}" for c in copied}
    insert_values |= {sk: f"xxhash64(s.{spec.key}, s.valid_from)", "valid_to": "CAST(NULL AS DATE)",
                      "is_current": "true"}
    dt = DeltaTable.forName(spark, table)
    (
        dt.alias("t")
        .merge(staged.alias("s"), f"t.{spec.key} = s.merge_key AND t.is_current")
        .whenMatchedUpdate(condition="t.attr_hash <> s.attr_hash",
                           set={"is_current": "false", "valid_to": "s.valid_from"})
        .whenNotMatchedInsert(values=insert_values)
        .whenNotMatchedBySourceUpdate(condition="t.is_current",
                                      set={"is_current": "false", "valid_to": f"DATE'{valid_from}'"})
        .execute()
    )
    m = dt.history(1).select("operationMetrics").first()[0] or {}
    stats = {
        "inserted": int(m.get("numTargetRowsInserted", 0)),
        "closed": int(m.get("numTargetRowsUpdated", 0)),
    }
    log.info("%s: %s", table, stats)
    return stats


def feed_start_date(spark: SparkSession, feed_version: str, catalog: str, schema: str) -> date:
    """feed_start_date of a release, from static_feed_info."""
    from pyspark.sql import functions as F

    row = (
        spark.table(f"{catalog}.{schema}.static_feed_info")
        .where(F.col("feed_version") == feed_version)
        .select(F.to_date("feed_start_date", "yyyyMMdd"))
        .first()
    )
    if row is None or row[0] is None:
        raise ValueError(f"no feed_start_date for feed_version {feed_version}")
    return row[0]


def load_dims(
    spark: SparkSession, feed_version: str, catalog: str = "workspace", schema: str = "transit"
) -> dict[str, dict[str, int]]:
    """Apply one static release to dim_routes and dim_stops."""
    from pyspark.sql import functions as F

    if not re.fullmatch(r"[\w.-]+", feed_version):
        raise ValueError(f"unexpected feed_version {feed_version!r}")
    valid_from = feed_start_date(spark, feed_version, catalog, schema)
    out: dict[str, dict[str, int]] = {}
    for name, spec in DIMS.items():
        source = spark.table(f"{catalog}.{schema}.{spec.source}").where(F.col("feed_version") == feed_version)
        src = prepare_source(source, spec, feed_version, valid_from)
        out[name] = apply_scd2(spark, src, f"{catalog}.{schema}.dim_{name}", spec)
    return out


ENRICHED_VP_VIEW_SQL = """
CREATE OR REPLACE VIEW {c}.{s}.silver_vehicle_positions_enriched AS
SELECT vp.*, r.route_sk, r.route_short_name, r.route_long_name
FROM {c}.{s}.silver_vehicle_positions AS vp
LEFT JOIN {c}.{s}.dim_routes AS r
  ON vp.route_id = r.route_id
 AND vp.service_date >= r.valid_from
 AND (r.valid_to IS NULL OR vp.service_date < r.valid_to)
"""


def create_enriched_view(spark: SparkSession, catalog: str = "workspace", schema: str = "transit") -> None:
    """Vehicle positions joined to the route version in force on each row's service_date."""
    spark.sql(ENRICHED_VP_VIEW_SQL.format(c=catalog, s=schema))
