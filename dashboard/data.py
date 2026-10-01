"""Load the exported gold snapshot and shape it for the dashboard (pandas only, no Spark).

Every percentage is recomputed from counts (on_time / departures), never by
averaging percentages: averaging a route with 3 departures and a route with
3,000 as if they were equal would overstate small routes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

LOCAL_TZ = "America/New_York"
DATASETS = (
    "route_summary",
    "route_hour",
    "daily",
    "stops",
    "weather",
    "bunching_example",
    "pipeline_runs",
    "dq_checks",
)
DEFAULT_DIR = Path(__file__).resolve().parent / "snapshot"


@dataclass
class Snapshot:
    """The CSVs and manifest of one export. Missing files load as empty frames."""

    manifest: dict[str, Any]
    frames: dict[str, pd.DataFrame] = field(default_factory=dict)

    def __getitem__(self, name: str) -> pd.DataFrame:
        return self.frames.get(name, pd.DataFrame())

    @property
    def metrics(self) -> dict[str, Any]:
        return self.manifest.get("metrics") or {}


def load_snapshot(directory: Path = DEFAULT_DIR) -> Snapshot | None:
    """Read manifest.json and every dataset CSV in `directory`; None if there is no manifest."""
    manifest_path = directory / "manifest.json"
    if not manifest_path.exists():
        return None
    frames = {}
    for name in DATASETS:
        path = directory / f"{name}.csv"
        if path.exists() and path.stat().st_size > 0:
            frames[name] = pd.read_csv(path, dtype={"route_id": str, "stop_id": str, "route_short_name": str})
    return Snapshot(json.loads(manifest_path.read_text()), frames)


def pct(part: float, whole: float) -> float | None:
    """part / whole as a percentage, None when there is nothing to divide by."""
    return round(100.0 * part / whole, 1) if whole else None


def rush_mask(df: pd.DataFrame, rush_hours: tuple[int, ...]) -> pd.Series:
    """Weekday rows in the rush hours."""
    return (df["day_type"] == "weekday") & df["hour_local"].isin(rush_hours)


def kpis(route_hour: pd.DataFrame, rush_hours: tuple[int, ...]) -> dict[str, float | int | None]:
    """Whole-system headline numbers, overall and at weekday rush hour."""
    if route_hour.empty:
        return {}
    rush = route_hour[rush_mask(route_hour, rush_hours)]
    rest = route_hour[~rush_mask(route_hour, rush_hours)]
    t = route_hour.sum(numeric_only=True)
    return {
        "departures": int(t["departures"]),
        "pct_on_time": pct(t["on_time"], t["departures"]),
        "pct_late": pct(t["late"], t["departures"]),
        "pct_early": pct(t["early"], t["departures"]),
        "avg_delay_min": round(t["delay_s_sum"] / t["departures"] / 60, 1) if t["departures"] else None,
        "headways": int(t["headways"]),
        "pct_bunched": pct(t["bunched"], t["headways"]),
        "pct_gapped": pct(t["gapped"], t["headways"]),
        "rush_pct_on_time": pct(rush["on_time"].sum(), rush["departures"].sum()),
        "rest_pct_on_time": pct(rest["on_time"].sum(), rest["departures"].sum()),
        "rush_pct_bunched": pct(rush["bunched"].sum(), rush["headways"].sum()),
        "rest_pct_bunched": pct(rest["bunched"].sum(), rest["headways"].sum()),
    }


def route_table(route_summary: pd.DataFrame, min_departures: int) -> pd.DataFrame:
    """One row per route with enough graded departures, worst rush-hour on-time % first.

    Routes below `min_departures` are dropped: with a handful of departures one
    late bus swings the percentage by tens of points.
    """
    if route_summary.empty:
        return route_summary
    df = route_summary[route_summary["departures"] >= min_departures].copy()
    name = df["route_short_name"].fillna(df["route_id"]).astype(str)
    df["route"] = name + " " + df["route_long_name"].fillna("").astype(str)
    df["route"] = df["route"].str.strip()
    df["pct_on_time"] = (100.0 * df["on_time"] / df["departures"]).round(1)
    df["pct_late"] = (100.0 * df["late"] / df["departures"]).round(1)
    df["rush_pct_on_time"] = (
        100.0 * df["rush_on_time"] / df["rush_departures"].where(df["rush_departures"] > 0)
    ).round(1)
    df["pct_bunched"] = (100.0 * df["bunched"] / df["headways"].where(df["headways"] > 0)).round(1)
    df["avg_delay_min"] = (df["avg_delay_s"] / 60).round(1)
    df["p90_delay_min"] = (df["p90_delay_s"] / 60).round(1)
    return df.sort_values(["pct_on_time", "departures"], ascending=[True, False]).reset_index(drop=True)


def hour_profile(route_hour: pd.DataFrame, day_type: str, route_id: str | None = None) -> pd.DataFrame:
    """On-time % and bunched % by scheduled hour for one day type, for one route or the whole system."""
    df = route_hour[route_hour["day_type"] == day_type]
    if route_id is not None:
        df = df[df["route_id"] == route_id]
    g = df.groupby("hour_local", as_index=False)[
        ["departures", "on_time", "late", "headways", "bunched", "delay_s_sum"]
    ].sum()
    g["pct_on_time"] = (100.0 * g["on_time"] / g["departures"].where(g["departures"] > 0)).round(1)
    g["pct_late"] = (100.0 * g["late"] / g["departures"].where(g["departures"] > 0)).round(1)
    g["pct_bunched"] = (100.0 * g["bunched"] / g["headways"].where(g["headways"] > 0)).round(1)
    g["avg_delay_min"] = (g["delay_s_sum"] / g["departures"].where(g["departures"] > 0) / 60).round(1)
    return g.sort_values("hour_local").reset_index(drop=True)


def route_hour_grid(
    route_hour: pd.DataFrame, route_ids: list[str], day_type: str, min_departures: int
) -> pd.DataFrame:
    """Route x hour cells with on-time %, blank where a cell has too few departures to mean anything."""
    df = route_hour[(route_hour["day_type"] == day_type) & route_hour["route_id"].isin(route_ids)].copy()
    df = df[df["departures"] >= min_departures]
    df["pct_on_time"] = (100.0 * df["on_time"] / df["departures"]).round(1)
    return df[["route_id", "hour_local", "departures", "pct_on_time"]]


def stop_points(stops: pd.DataFrame, min_departures: int) -> pd.DataFrame:
    """Graded stops with a location and enough departures, with on-time % and bunched %."""
    if stops.empty:
        return stops
    df = stops.dropna(subset=["lat", "lon"])
    df = df[df["departures"] >= min_departures].copy()
    df["pct_on_time"] = (100.0 * df["on_time"] / df["departures"]).round(1)
    df["pct_bunched"] = (100.0 * df["bunched"] / df["headways"].where(df["headways"] > 0)).round(1)
    return df.reset_index(drop=True)


def weather_table(weather: pd.DataFrame) -> pd.DataFrame:
    """Wet vs dry, all hours and weekday rush, with on-time % and the sample behind each number."""
    if weather.empty:
        return weather
    df = weather.copy()
    df["rush"] = df["rush"].astype(str).str.lower().isin(["true", "1"])
    df["period"] = df["rush"].map({True: "Weekday rush", False: "Other hours"})
    df["pct_on_time"] = (100.0 * df["on_time"] / df["departures"].where(df["departures"] > 0)).round(1)
    df["avg_delay_min"] = (df["avg_delay_s"] / 60).round(1)
    return df[["period", "weather", "hours", "departures", "pct_on_time", "avg_delay_min"]]


def bunching_timeline(example: pd.DataFrame) -> pd.DataFrame:
    """The example stop-day as long rows: one 'Scheduled' and one 'Actual' point per bus, local time."""
    if example.empty:
        return example
    rows = []
    for _, r in example.iterrows():
        for kind, col in (("Scheduled", "scheduled_epoch"), ("Actual", "observed_epoch")):
            rows.append(
                {
                    "kind": kind,
                    "trip_id": r["trip_id"],
                    "vehicle_id": r["vehicle_id"],
                    "epoch": r[col],
                    "delay_min": round(r["delay_s"] / 60, 1),
                    "headway_status": r.get("headway_status"),
                }
            )
    out = pd.DataFrame(rows)
    out["time"] = (
        pd.to_datetime(out["epoch"], unit="s", utc=True).dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)
    )
    return out


def events_per_day(runs: pd.DataFrame) -> pd.DataFrame:
    """Bronze rows added per Eastern calendar day (succeeded attempts only), from the run log."""
    if runs.empty:
        return runs
    df = runs[(runs["step"] == "bronze") & (runs["status"] == "succeeded")].copy()
    df["date"] = pd.to_datetime(df["started_epoch"], unit="s", utc=True).dt.tz_convert(LOCAL_TZ).dt.date
    return df.groupby("date", as_index=False).agg(events=("bronze_rows", "sum"), runs=("run_id", "nunique"))


def step_runtimes(runs: pd.DataFrame) -> pd.DataFrame:
    """Median and max seconds per step, succeeded attempts only."""
    if runs.empty:
        return runs
    ok = runs[runs["status"] == "succeeded"]
    return ok.groupby("step", as_index=False).agg(
        median_s=("duration_s", "median"), max_s=("duration_s", "max"), attempts=("run_id", "count")
    )


def freshness_series(runs: pd.DataFrame) -> pd.DataFrame:
    """Freshness (minutes) at each successful gold build, in local time."""
    if runs.empty:
        return runs
    df = (
        runs[(runs["step"] == "gold") & (runs["status"] == "succeeded")].dropna(subset=["freshness_s"]).copy()
    )
    df["time"] = (
        pd.to_datetime(df["started_epoch"], unit="s", utc=True).dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)
    )
    df["freshness_min"] = (df["freshness_s"] / 60).round(1)
    return df[["time", "freshness_min"]]
