"""Shared fixtures. Spark tests run on a local Spark + Delta session and are skipped
when pyspark, delta-spark or Java is not installed (pip install -r requirements-spark.txt)."""
from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[object]:
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    if shutil.which("java") is None:
        pytest.skip("Java is required for local Spark tests")
    from pyspark.sql import SparkSession

    warehouse: Path = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("dc-transit-pulse-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
    )
    session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
