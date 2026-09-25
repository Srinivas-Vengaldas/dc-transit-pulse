"""Load a WMATA GTFS static zip into Delta tables, one table per GTFS file.

Runs on Databricks. Every table gets a `feed_version` column (from
feed_info.txt), and each load replaces only the rows of that version:

    load 1 of S1000251  -> rows for S1000251 written
    load 2 of S1000251  -> the same rows replaced, no duplicates (idempotent)
    load of S1000300    -> new rows added next to S1000251 (history kept)

Keeping every version side by side is what the Week 2 SCD Type 2 route and
stop dimensions will be built from.

Usage in a Databricks notebook:
    import sys
    sys.path.append("/Workspace/Users/<you>/dc-transit-pulse")
    from pipelines.static_to_delta import load_static
    load_static(spark, "/Volumes/workspace/transit/raw/static/bus_gtfs_static_S1000251.zip",
                catalog="workspace", schema="transit")
"""
from __future__ import annotations

import csv
import io
import logging
import zipfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pyspark is only needed on Databricks, not for the unit tests
    from pyspark.sql import SparkSession

log = logging.getLogger("static_to_delta")


def read_feed_version(zip_path: str | Path) -> str:
    """Return feed_version from feed_info.txt inside a GTFS zip."""
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open("feed_info.txt") as fh:
            # utf-8-sig strips the byte-order mark some GTFS exporters add
            row = next(csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig")))
    version = (row.get("feed_version") or "").strip()
    if not version:
        raise ValueError(f"{zip_path}: feed_info.txt has no feed_version")
    return version


def extract_gtfs(zip_path: str | Path, dest_dir: str | Path) -> list[str]:
    """Extract the top-level .txt files of a GTFS zip; return their table names.

    Only the base name of each member is used, so a malicious entry such as
    '../../etc/x.txt' cannot write outside dest_dir ("zip slip").
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    names: list[str] = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            base = PurePosixPath(member.filename).name
            if member.is_dir() or not base.endswith(".txt"):
                continue
            (dest / base).write_bytes(zf.read(member))
            names.append(base.removesuffix(".txt"))
    return sorted(names)


def load_static(
    spark: SparkSession,
    zip_path: str,
    catalog: str = "workspace",
    schema: str = "transit",
    extract_root: str | None = None,
) -> dict[str, int]:
    """Extract the zip next to itself and write each file to <catalog>.<schema>.static_<name>.

    Columns are kept as strings: GTFS times like 25:10:00 are not valid
    timestamps, and typing belongs in silver. Returns rows written per table.
    """
    from pyspark.sql import functions as F

    version = read_feed_version(zip_path)
    root = extract_root or str(PurePosixPath(zip_path).parent / "extracted")
    dest = f"{root}/feed_version={version}"
    tables = extract_gtfs(zip_path, dest)
    log.info("feed_version=%s: %d files extracted to %s", version, len(tables), dest)

    counts: dict[str, int] = {}
    for name in tables:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(f"{dest}/{name}.txt")
            .withColumn("feed_version", F.lit(version))
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_loaded_at", F.current_timestamp())
        )
        table = f"{catalog}.{schema}.static_{name}"
        writer = df.write.format("delta")
        if spark.catalog.tableExists(table):
            # Replace only this version's rows: rerunning is safe, older versions stay.
            writer.mode("overwrite").option("replaceWhere", f"feed_version = '{version}'").saveAsTable(table)
        else:
            writer.mode("errorifexists").saveAsTable(table)
        counts[name] = spark.table(table).where(F.col("feed_version") == version).count()
        log.info("%s: %d rows", table, counts[name])
    return counts
