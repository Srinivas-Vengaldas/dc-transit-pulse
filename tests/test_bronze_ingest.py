"""Tests for the Auto Loader configuration (Auto Loader itself only runs on Databricks)."""
from __future__ import annotations

import pytest

from pipelines.bronze_ingest import autoloader_options, ingest_feed, paths_for


def test_paths_keep_state_next_to_data_per_feed() -> None:
    p = paths_for("/Volumes/workspace/transit/raw/", "trip_updates")
    assert p == {
        "source": "/Volumes/workspace/transit/raw/landing/trip_updates",
        "schema": "/Volumes/workspace/transit/raw/_autoloader/trip_updates/schema",
        "checkpoint": "/Volumes/workspace/transit/raw/_autoloader/trip_updates/checkpoint",
    }


def test_options_keep_bronze_raw_and_evolvable() -> None:
    opts = autoloader_options("/x/schema")
    assert opts["cloudFiles.format"] == "json"
    assert opts["cloudFiles.schemaLocation"] == "/x/schema"
    assert opts["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    assert opts["cloudFiles.inferColumnTypes"] == "false"
    assert opts["pathGlobFilter"] == "*.jsonl"


def test_unknown_feed_rejected() -> None:
    with pytest.raises(ValueError, match="unknown feed"):
        ingest_feed(spark=None, feed="alerts", raw_volume="/Volumes/x")  # type: ignore[arg-type]


def test_input_rows_reads_dict_or_object() -> None:
    from types import SimpleNamespace

    from pipelines.bronze_ingest import _input_rows

    assert _input_rows({"numInputRows": 5}) == 5
    assert _input_rows(SimpleNamespace(numInputRows=7)) == 7
