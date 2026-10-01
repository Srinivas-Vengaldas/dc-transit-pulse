"""Tests for GTFS time parsing and the typed silver stop_times."""
from __future__ import annotations

import pytest

from pipelines.static_silver import gtfs_time_to_seconds, transform_stop_times


@pytest.mark.parametrize(
    ("value", "seconds"),
    [("7:00:00", 25200), ("07:00:00", 25200), ("0:00:00", 0), ("24:00:00", 86400),
     ("25:10:00", 90600), (" 8:05:09 ", 29109), ("", None), (None, None), ("7:60:00", None), ("abc", None)],
)
def test_gtfs_time_to_seconds(value: str | None, seconds: int | None) -> None:
    assert gtfs_time_to_seconds(value) == seconds


def test_text_comparison_trap() -> None:
    """Why times are parsed: as text, 7 am sorts after midnight-plus-one-hour."""
    assert "7:00:00" > "25:00:00"
    assert gtfs_time_to_seconds("7:00:00") < gtfs_time_to_seconds("25:00:00")


def test_spark_parsing_matches_python(spark) -> None:
    values = ["7:00:00", "07:00:00", "24:00:00", "25:10:00", "", None, "7:60:00"]
    rows = [("V1", "T1", "S1", str(i + 1), v, v, "1") for i, v in enumerate(values)]
    df = spark.createDataFrame(rows, "feed_version string, trip_id string, stop_id string, "
                                     "stop_sequence string, arrival_time string, departure_time string, "
                                     "timepoint string")
    out = transform_stop_times(df).orderBy("stop_sequence").collect()
    assert [r.arrival_secs for r in out] == [gtfs_time_to_seconds(v) for v in values]
    assert out[0].stop_sequence == 1 and out[0].timepoint == 1
    assert out[0].shape_dist_traveled is None
