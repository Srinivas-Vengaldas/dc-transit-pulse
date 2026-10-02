"""Tests for the dashboard's data shaping and the snapshot fetch (pandas only, no Spark or Streamlit)."""

from __future__ import annotations

import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

pd = pytest.importorskip("pandas")

from dashboard import data  # noqa: E402
from dashboard.fetch_snapshot import FILES, fetch  # noqa: E402

RUSH = (6, 7, 8, 16, 17, 18)


def rh(
    route: str, day_type: str, hour: int, dep: int, on_time: int, headways: int = 0, bunched: int = 0
) -> dict:
    return {
        "route_id": route,
        "day_type": day_type,
        "hour_local": hour,
        "departures": dep,
        "on_time": on_time,
        "late": dep - on_time,
        "early": 0,
        "delay_s_sum": 60 * dep,
        "headways": headways,
        "bunched": bunched,
        "gapped": 0,
    }


ROUTE_HOUR = pd.DataFrame(
    [
        rh("A", "weekday", 7, 10, 5, 8, 4),  # rush
        rh("A", "weekday", 12, 30, 27, 20, 0),
        rh("B", "weekend", 7, 60, 30, 10, 1),  # weekend 7 a.m. is not rush
    ]
)


def test_pct() -> None:
    assert data.pct(1, 4) == 25.0 and data.pct(1, 0) is None


def test_kpis_weight_by_count_not_by_row() -> None:
    k = data.kpis(ROUTE_HOUR, RUSH)
    assert k["departures"] == 100
    assert k["pct_on_time"] == 62.0  # 62 of 100, not the mean of 50%, 90% and 50%
    assert k["rush_pct_on_time"] == 50.0 and k["rest_pct_on_time"] == pytest.approx(63.3)
    assert k["pct_bunched"] == pytest.approx(13.2)  # 5 of 38
    assert k["avg_delay_min"] == 1.0
    assert data.kpis(ROUTE_HOUR.iloc[0:0], RUSH) == {}


def test_hour_profile_for_one_route_and_system() -> None:
    sys_wd = data.hour_profile(ROUTE_HOUR, "weekday")
    assert sys_wd["hour_local"].tolist() == [7, 12]
    assert sys_wd["pct_on_time"].tolist() == [50.0, 90.0]
    assert data.hour_profile(ROUTE_HOUR, "weekday", "B").empty


def test_route_table_filters_small_samples_and_sorts_worst_first() -> None:
    base = {"early": 0, "p90_delay_s": 300, "days": 2, "gapped": 0}
    summary = pd.DataFrame(
        [
            {
                **base,
                "route_id": "A",
                "route_short_name": "A",
                "route_long_name": "Alpha",
                "departures": 100,
                "on_time": 90,
                "late": 10,
                "avg_delay_s": 60,
                "rush_departures": 0,
                "rush_on_time": 0,
                "headways": 0,
                "bunched": 0,
            },
            {
                **base,
                "route_id": "B",
                "route_short_name": None,
                "route_long_name": None,
                "departures": 80,
                "on_time": 40,
                "late": 40,
                "avg_delay_s": 120,
                "rush_departures": 10,
                "rush_on_time": 2,
                "headways": 10,
                "bunched": 3,
            },
            {
                **base,
                "route_id": "C",
                "route_short_name": "C",
                "route_long_name": "Tiny",
                "departures": 3,
                "on_time": 0,
                "late": 3,
                "avg_delay_s": 900,
                "rush_departures": 0,
                "rush_on_time": 0,
                "headways": 0,
                "bunched": 0,
            },
        ]
    )
    t = data.route_table(summary, min_departures=50)
    assert t["route_id"].tolist() == ["B", "A"]  # C has too few departures; B is worse
    assert t.loc[0, "route"] == "B"  # missing names fall back to the id
    assert t.loc[0, "rush_pct_on_time"] == 20.0 and t.loc[0, "pct_bunched"] == 30.0
    assert pd.isna(t.loc[1, "rush_pct_on_time"])  # no rush departures: blank, not 0% or a division error


def test_weather_table_reads_csv_booleans() -> None:
    w = pd.DataFrame(
        [{"weather": "wet", "rush": "True", "hours": 2, "departures": 10, "on_time": 4, "avg_delay_s": 120.0}]
    )
    out = data.weather_table(w)
    assert out.loc[0, "period"] == "Weekday rush" and out.loc[0, "pct_on_time"] == 40.0


def test_bunching_timeline_has_scheduled_and_actual_in_local_time() -> None:
    ex = pd.DataFrame(
        [
            {
                "trip_id": "T1",
                "vehicle_id": "V1",
                "scheduled_epoch": 1790000000,
                "observed_epoch": 1790000060,
                "delay_s": 60,
                "headway_status": None,
            }
        ]
    )
    tl = data.bunching_timeline(ex)
    assert tl["kind"].tolist() == ["Scheduled", "Actual"]
    assert str(tl.loc[0, "time"]) == "2026-09-21 10:13:20"  # 14:13:20 UTC = 10:13:20 EDT


def test_pipeline_series() -> None:
    runs = pd.DataFrame(
        [
            {
                "run_id": "r1",
                "step": "bronze",
                "status": "succeeded",
                "started_epoch": 1790000000,
                "duration_s": 30,
                "bronze_rows": 100,
                "freshness_s": None,
            },
            {
                "run_id": "r1",
                "step": "bronze",
                "status": "failed",
                "started_epoch": 1789999900,
                "duration_s": 5,
                "bronze_rows": 0,
                "freshness_s": None,
            },
            {
                "run_id": "r1",
                "step": "gold",
                "status": "succeeded",
                "started_epoch": 1790000100,
                "duration_s": 20,
                "bronze_rows": 0,
                "freshness_s": 360,
            },
        ]
    )
    assert data.events_per_day(runs)["events"].tolist() == [100]
    assert data.freshness_series(runs)["freshness_min"].tolist() == [6.0]
    rt = data.step_runtimes(runs).set_index("step")
    assert rt.loc["bronze", "median_s"] == 30  # the failed attempt is not a runtime sample


def test_load_snapshot(tmp_path: Path) -> None:
    assert data.load_snapshot(tmp_path) is None
    (tmp_path / "manifest.json").write_text(json.dumps({"metrics": {"x": 1}}))
    pd.DataFrame([{"route_id": "070", "departures": 1}]).to_csv(tmp_path / "route_summary.csv", index=False)
    snap = data.load_snapshot(tmp_path)
    assert snap.metrics == {"x": 1}
    assert snap["route_summary"].loc[0, "route_id"] == "070"  # ids stay text: leading zeros survive
    assert snap["stops"].empty


class FakeFiles:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files, self.order = files, []

    def download(self, file_path: str):
        name = file_path.rsplit("/", 1)[-1]
        self.order.append(name)
        if name not in self.files:
            raise FileNotFoundError(file_path)
        return SimpleNamespace(contents=io.BytesIO(self.files[name]))


def test_fetch_copies_files_and_writes_manifest_last(tmp_path: Path) -> None:
    files = FakeFiles({"manifest.json": b"{}", "daily.csv": b"a\n1\n"})
    sizes = fetch(files, "/Volumes/x/exports/dashboard/", tmp_path)
    assert sizes == {"daily.csv": 4, "manifest.json": 2}
    assert files.order[-1] == "manifest.json" and len(files.order) == len(FILES)
    assert (tmp_path / "daily.csv").read_text() == "a\n1\n"
    assert not list(tmp_path.glob(".*.tmp"))


def test_fetch_fails_without_manifest(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        fetch(FakeFiles({}), "/Volumes/x", tmp_path)
