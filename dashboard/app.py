"""DC Transit Pulse dashboard: how reliable are DC's buses, and when and where do they bunch?

Reads the gold snapshot in dashboard/snapshot/ (CSV files written by the
`export` job task and copied here by fetch_snapshot.py). No Spark and no
Databricks connection, so it runs free on Streamlit Community Cloud.

    streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard import data  # noqa: E402

# Colors (validated default data-viz palette): categorical slots in fixed order, one sequential hue.
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
GRAY = "#898781"
STATUS_COLORS = {"On time": BLUE, "Late": ORANGE, "Early": AQUA}
SEQ_BLUE = ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]
RED, MID = (227, 73, 72), (200, 199, 194)
BLUE_RGB = (42, 120, 214)
DEFAULT_RUSH = (6, 7, 8, 16, 17, 18)

st.set_page_config(page_title="DC Transit Pulse", page_icon="🚌", layout="wide")


@st.cache_data
def load(directory: str) -> data.Snapshot | None:
    return data.load_snapshot(Path(directory))


def fmt_pct(v: float | None) -> str:
    return "n/a" if v is None or pd.isna(v) else f"{v:.1f}%"


def hour_axis(title: str = "Scheduled hour (Eastern)") -> alt.X:
    return alt.X("hour_local:O", title=title, axis=alt.Axis(labelAngle=0))


def chart(c: alt.Chart) -> None:
    st.altair_chart(c, width="stretch")


# --------------------------------------------------------------------------- load

snap = load(os.environ.get("DASHBOARD_SNAPSHOT_DIR", str(data.DEFAULT_DIR)))
st.title("DC Transit Pulse")
st.caption(
    "How reliable are Washington, DC's Metrobuses, and when and where do they bunch? "
    "Measured from WMATA's live bus feeds, not from the timetable."
)

if snap is None:
    st.warning(
        "No snapshot yet. Run the `export` job task, then `python -m dashboard.fetch_snapshot` "
        "(see README), and reload."
    )
    st.stop()

defs = snap.manifest.get("definitions", {})
rush_hours = tuple(defs.get("rush_hours", DEFAULT_RUSH))
early_min, late_min = defs.get("early_s", 120) // 60, defs.get("late_s", 420) // 60
dates = snap.manifest.get("service_dates") or {}
route_hour = snap["route_hour"]
k = data.kpis(route_hour, rush_hours)
if not k:
    st.warning("The snapshot has no graded departures yet.")
    st.stop()

st.caption(
    f"Data: service dates {dates.get('first_service_date', '?')} to {dates.get('last_service_date', '?')}, "
    f"{k['departures']:,} graded departures, {k['headways']:,} measured gaps between buses. "
    f"Snapshot generated {snap.manifest.get('generated_at_utc', '?')} UTC."
)

tabs = st.tabs(["The problem", "Routes", "When", "Where", "Rain", "How it's built"])

# --------------------------------------------------------------------------- the problem

with tabs[0]:
    st.subheader("Riders plan around the timetable. The buses don't always follow it.")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Departures on time",
        fmt_pct(k["pct_on_time"]),
        help=f"Left a timepoint between {early_min} min early and {late_min} min late.",
    )
    c2.metric(
        "On time at weekday rush hour",
        fmt_pct(k["rush_pct_on_time"]),
        delta=None
        if k["rush_pct_on_time"] is None or k["rest_pct_on_time"] is None
        else f"{k['rush_pct_on_time'] - k['rest_pct_on_time']:+.1f} pts vs other hours",
        help="Rush = 6-9 a.m. and 4-7 p.m. on weekdays, by scheduled hour.",
    )
    c3.metric(
        "Gaps where buses bunched",
        fmt_pct(k["pct_bunched"]),
        help="The gap to the previous bus was under 25% of the scheduled gap: two buses arrive together.",
    )
    c4.metric(
        "Average delay",
        "n/a" if k["avg_delay_min"] is None else f"{k['avg_delay_min']:+.1f} min",
        help="Observed minus scheduled departure, averaged over every graded departure.",
    )

    st.markdown(
        "**Why it matters.** When a bus runs late it picks up the riders waiting for the next one, "
        "so it slows down further while the bus behind it catches up. The result is *bunching*: "
        "two buses arrive together, then nobody comes for twice the scheduled gap. A rider sees a "
        "15-minute route that feels like a 30-minute one."
    )

    tl = data.bunching_timeline(snap["bunching_example"])
    if not tl.empty:
        ex = snap["bunching_example"].iloc[0]
        stop_name = ex.get("stop_name") if isinstance(ex.get("stop_name"), str) else ex["stop_id"]
        st.markdown(
            f"**A real example: route {ex['route_id']} at {stop_name}, {ex['service_date']}.** "
            "Each dot is one bus. Top row: when the timetable said it would leave. "
            "Bottom row: when it actually did. The lines connect the same bus."
        )
        base = alt.Chart(tl)
        lines = base.mark_line(color=GRAY, strokeWidth=1, opacity=0.6).encode(
            x="time:T", y=alt.Y("kind:N", sort=["Scheduled", "Actual"], title=None), detail="trip_id:N"
        )
        dots = base.mark_circle(size=110, opacity=1, stroke="#fcfcfb", strokeWidth=2).encode(
            x=alt.X("time:T", title="Time (Eastern)", axis=alt.Axis(format="%H:%M")),
            y=alt.Y("kind:N", sort=["Scheduled", "Actual"], title=None),
            color=alt.Color(
                "kind:N",
                scale=alt.Scale(domain=["Scheduled", "Actual"], range=[GRAY, BLUE]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=[
                alt.Tooltip("kind:N", title=" "),
                alt.Tooltip("time:T", format="%H:%M:%S", title="Time"),
                alt.Tooltip("vehicle_id:N", title="Bus"),
                alt.Tooltip("delay_min:Q", title="Delay (min)"),
                alt.Tooltip("headway_status:N", title="Gap to previous bus"),
            ],
        )
        chart((lines + dots).properties(height=220))
        n_bunched = int((snap["bunching_example"]["headway_status"] == "bunched").sum())
        st.caption(
            f"{len(snap['bunching_example'])} buses at this stop that day; {n_bunched} arrived "
            "bunched with the bus ahead. Chosen automatically as the stop-day with the most bunching."
        )

    st.markdown(
        "**What this project does.** It collects every bus position WMATA publishes (about every 30 s), "
        "works out when each bus actually left each timepoint, compares that with the schedule in force "
        "that day, and measures the gap between consecutive buses. The tabs answer the questions a "
        "planner or rider would ask: *which routes*, *when*, *where*, and *does rain make it worse*."
    )

# --------------------------------------------------------------------------- routes

with tabs[1]:
    st.subheader("Which routes are least reliable?")
    min_dep = st.slider(
        "Only routes with at least this many graded departures",
        10,
        500,
        50,
        step=10,
        help="Small samples swing wildly: one late bus out of 5 is 20 points.",
    )
    routes = data.route_table(snap["route_summary"], min_dep)
    if routes.empty:
        st.info("No route has that many graded departures yet. Lower the threshold.")
    else:
        worst = routes.head(15)
        st.markdown(
            f"**The 15 routes with the lowest on-time share** (of {len(routes)} routes with "
            f"at least {min_dep} departures)."
        )
        bars = (
            alt.Chart(worst)
            .mark_bar(color=BLUE, cornerRadiusEnd=4, height=14)
            .encode(
                x=alt.X("pct_on_time:Q", title="Departures on time (%)", scale=alt.Scale(domain=[0, 100])),
                y=alt.Y("route:N", sort=None, title=None, axis=alt.Axis(labelOverlap=False, labelLimit=260)),
                tooltip=[
                    alt.Tooltip("route:N", title="Route"),
                    alt.Tooltip("pct_on_time:Q", title="On time %"),
                    alt.Tooltip("pct_late:Q", title="Late %"),
                    alt.Tooltip("rush_pct_on_time:Q", title="On time % at rush"),
                    alt.Tooltip("pct_bunched:Q", title="Bunched %"),
                    alt.Tooltip("departures:Q", title="Departures", format=","),
                ],
            )
        )
        labels = bars.mark_text(align="left", dx=4, color="#52514e").encode(
            text=alt.Text("pct_on_time:Q", format=".0f")
        )
        chart((bars + labels).properties(height=max(200, 28 * len(worst))))

        st.markdown("**Every route** (sort by any column)")
        st.dataframe(
            routes[
                [
                    "route",
                    "departures",
                    "pct_on_time",
                    "pct_late",
                    "rush_pct_on_time",
                    "avg_delay_min",
                    "p90_delay_min",
                    "pct_bunched",
                    "days",
                ]
            ],
            hide_index=True,
            width="stretch",
            column_config={
                "route": "Route",
                "departures": st.column_config.NumberColumn("Departures", format="%d"),
                "pct_on_time": st.column_config.NumberColumn("On time %", format="%.1f"),
                "pct_late": st.column_config.NumberColumn("Late %", format="%.1f"),
                "rush_pct_on_time": st.column_config.NumberColumn("On time % (rush)", format="%.1f"),
                "avg_delay_min": st.column_config.NumberColumn("Avg delay (min)", format="%.1f"),
                "p90_delay_min": st.column_config.NumberColumn("90th pct delay (min)", format="%.1f"),
                "pct_bunched": st.column_config.NumberColumn("Bunched %", format="%.1f"),
                "days": st.column_config.NumberColumn("Days", format="%d"),
            },
        )

        st.markdown("**One route through the day** (weekdays), against the whole system")
        pick = st.selectbox(
            "Route",
            routes["route_id"].tolist(),
            format_func=lambda r: routes.set_index("route_id").loc[r, "route"],
        )
        mine = data.hour_profile(route_hour, "weekday", pick).assign(series=f"Route {pick}")
        system = data.hour_profile(route_hour, "weekday").assign(series="All routes")
        both = pd.concat([mine, system]).dropna(subset=["pct_on_time"])
        chart(
            alt.Chart(both)
            .mark_line(point=alt.OverlayMarkDef(size=64), strokeWidth=2)
            .encode(
                x=hour_axis(),
                y=alt.Y("pct_on_time:Q", title="On time (%)", scale=alt.Scale(domain=[0, 100])),
                color=alt.Color(
                    "series:N",
                    scale=alt.Scale(domain=[f"Route {pick}", "All routes"], range=[BLUE, GRAY]),
                    legend=alt.Legend(title=None, orient="top"),
                ),
                tooltip=[
                    alt.Tooltip("series:N", title=" "),
                    alt.Tooltip("hour_local:O", title="Hour"),
                    alt.Tooltip("pct_on_time:Q", title="On time %"),
                    alt.Tooltip("departures:Q", title="Departures", format=","),
                ],
            )
            .properties(height=280)
        )

# --------------------------------------------------------------------------- when

with tabs[2]:
    st.subheader("When does service break down?")
    prof = pd.concat(
        [
            data.hour_profile(route_hour, d).assign(day_type=d.capitalize() + "s")
            for d in ("weekday", "weekend")
        ]
    )
    prof = prof[prof["departures"] >= 20]
    left, right = st.columns(2)
    with left:
        st.markdown("**On-time share by hour**")
        chart(
            alt.Chart(prof)
            .mark_line(point=alt.OverlayMarkDef(size=64), strokeWidth=2)
            .encode(
                x=hour_axis(),
                y=alt.Y("pct_on_time:Q", title="On time (%)", scale=alt.Scale(domain=[0, 100])),
                color=alt.Color(
                    "day_type:N",
                    scale=alt.Scale(domain=["Weekdays", "Weekends"], range=[BLUE, ORANGE]),
                    legend=alt.Legend(title=None, orient="top"),
                ),
                tooltip=[
                    "day_type:N",
                    alt.Tooltip("hour_local:O", title="Hour"),
                    alt.Tooltip("pct_on_time:Q", title="On time %"),
                    alt.Tooltip("avg_delay_min:Q", title="Avg delay (min)"),
                    alt.Tooltip("departures:Q", title="Departures", format=","),
                ],
            )
            .properties(height=300)
        )
    with right:
        st.markdown("**Share of gaps where buses bunched, by hour**")
        chart(
            alt.Chart(prof.dropna(subset=["pct_bunched"]))
            .mark_line(point=alt.OverlayMarkDef(size=64), strokeWidth=2)
            .encode(
                x=hour_axis(),
                y=alt.Y("pct_bunched:Q", title="Bunched (%)"),
                color=alt.Color(
                    "day_type:N",
                    scale=alt.Scale(domain=["Weekdays", "Weekends"], range=[BLUE, ORANGE]),
                    legend=alt.Legend(title=None, orient="top"),
                ),
                tooltip=[
                    "day_type:N",
                    alt.Tooltip("hour_local:O", title="Hour"),
                    alt.Tooltip("pct_bunched:Q", title="Bunched %"),
                    alt.Tooltip("headways:Q", title="Gaps measured", format=","),
                ],
            )
            .properties(height=300)
        )

    routes_all = data.route_table(snap["route_summary"], 50)
    if not routes_all.empty:
        st.markdown(
            "**The 20 least reliable routes, hour by hour (weekdays).** Darker = more departures on time; "
            "blank = fewer than 5 departures in that hour."
        )
        grid = data.route_hour_grid(route_hour, routes_all.head(20)["route_id"].tolist(), "weekday", 5)
        chart(
            alt.Chart(grid)
            .mark_rect(stroke="#fcfcfb", strokeWidth=2)
            .encode(
                x=hour_axis(),
                y=alt.Y(
                    "route_id:N",
                    sort=routes_all.head(20)["route_id"].tolist(),
                    title="Route",
                    axis=alt.Axis(labelOverlap=False),
                ),
                color=alt.Color(
                    "pct_on_time:Q", title="On time %", scale=alt.Scale(domain=[0, 100], range=SEQ_BLUE)
                ),
                tooltip=[
                    alt.Tooltip("route_id:N", title="Route"),
                    alt.Tooltip("hour_local:O", title="Hour"),
                    alt.Tooltip("pct_on_time:Q", title="On time %"),
                    alt.Tooltip("departures:Q", title="Departures"),
                ],
            )
            .properties(height=26 * 20)
        )

# --------------------------------------------------------------------------- where

with tabs[3]:
    st.subheader("Where are buses least reliable?")
    min_stop = st.slider("Only stops with at least this many graded departures", 5, 200, 20, step=5)
    pts = data.stop_points(snap["stops"], min_stop)
    if pts.empty:
        st.info("No stop has that many graded departures yet.")
    else:
        avg = k["pct_on_time"] or 50.0

        def color(p: float) -> list[int]:
            """Diverging around the system average: blue better, red worse, gray at the average."""
            t = max(-1.0, min(1.0, (p - avg) / 25.0))
            end = BLUE_RGB if t >= 0 else RED
            return [round(MID[i] + (end[i] - MID[i]) * abs(t)) for i in range(3)] + [210]

        pts["color"] = pts["pct_on_time"].map(color)
        pts["radius"] = 60 + 140 * (pts["departures"] / pts["departures"].max()) ** 0.5
        st.markdown(
            f"Each dot is a timepoint stop, sized by departures graded there. **Blue** stops beat the "
            f"system average ({avg:.1f}% on time), **red** stops fall below it."
        )
        st.pydeck_chart(
            pdk.Deck(
                map_style=None,
                initial_view_state=pdk.ViewState(
                    latitude=float(pts["lat"].median()), longitude=float(pts["lon"].median()), zoom=10
                ),
                layers=[
                    pdk.Layer(
                        "ScatterplotLayer",
                        pts,
                        get_position=["lon", "lat"],
                        get_fill_color="color",
                        get_radius="radius",
                        pickable=True,
                        stroked=True,
                        get_line_color=[255, 255, 255],
                        line_width_min_pixels=1,
                    )
                ],
                tooltip={
                    "text": "{stop_name}\n{pct_on_time}% on time ({departures} departures)\n"
                    "Bunched: {pct_bunched}%\nRoutes: {route_ids}"
                },
            )
        )
        st.markdown("**Least reliable stops**")
        st.dataframe(
            pts.sort_values("pct_on_time").head(15)[
                ["stop_name", "route_ids", "departures", "pct_on_time", "avg_delay_s", "pct_bunched"]
            ],
            hide_index=True,
            width="stretch",
            column_config={
                "stop_name": "Stop",
                "route_ids": "Routes",
                "departures": "Departures",
                "pct_on_time": "On time %",
                "avg_delay_s": "Avg delay (s)",
                "pct_bunched": "Bunched %",
            },
        )

# --------------------------------------------------------------------------- rain

with tabs[4]:
    st.subheader("Do delays rise with rain?")
    wt = data.weather_table(snap["weather"])
    if wt.empty:
        st.info("No weather-joined data in this snapshot.")
    else:
        chart(
            alt.Chart(wt.dropna(subset=["pct_on_time"]))
            .mark_bar(cornerRadiusEnd=4)
            .encode(
                x=alt.X("weather:N", title=None, sort=["dry", "wet"], axis=alt.Axis(labelAngle=0)),
                y=alt.Y("pct_on_time:Q", title="On time (%)", scale=alt.Scale(domain=[0, 100])),
                color=alt.Color(
                    "weather:N", scale=alt.Scale(domain=["dry", "wet"], range=[GRAY, BLUE]), legend=None
                ),
                column=alt.Column("period:N", title=None, sort=["Weekday rush", "Other hours"]),
                tooltip=[
                    "period:N",
                    "weather:N",
                    alt.Tooltip("pct_on_time:Q", title="On time %"),
                    alt.Tooltip("avg_delay_min:Q", title="Avg delay (min)"),
                    alt.Tooltip("hours:Q", title="Hours"),
                    alt.Tooltip("departures:Q", format=","),
                ],
            )
            .properties(width=180, height=260)
        )
        st.dataframe(
            wt,
            hide_index=True,
            width="stretch",
            column_config={
                "period": "Period",
                "weather": "Weather",
                "hours": "Hours observed",
                "departures": "Departures",
                "pct_on_time": "On time %",
                "avg_delay_min": "Avg delay (min)",
            },
        )
        wet_hours = int(wt.loc[wt["weather"] == "wet", "hours"].sum())
        st.caption(
            f"A wet hour has at least 0.1 mm of precipitation (Open-Meteo, downtown DC). This snapshot "
            f"has {wet_hours} wet hours. With few wet hours the difference can be chance; read it as a "
            "pattern to watch, not a conclusion."
        )

# --------------------------------------------------------------------------- how it's built

with tabs[5]:
    st.subheader("How it's built")
    st.graphviz_chart("""
digraph {
  rankdir=LR; node [shape=box, style="rounded", fontname="Helvetica", fontsize=11];
  wmata [label="WMATA GTFS-RT\\nbus positions +\\ntrip updates"]; meteo [label="Open-Meteo\\nhourly weather"];
  static [label="WMATA GTFS\\nstatic schedule"];
  collect [label="collect\\n(20 polls, 30 s)"]; volume [label="Volume\\nJSON Lines"];
  bronze [label="bronze\\nAuto Loader"]; silver [label="silver\\nparse, dedupe,\\nwatermark, MERGE"];
  dims [label="SCD Type 2\\nroutes, stops"]; gold [label="gold\\ndepartures, OTP,\\nheadways"];
  quality [label="quality\\n12 SQL checks"]; export [label="export\\nCSV snapshot"];
  app [label="this dashboard"];
  wmata -> collect -> volume -> bronze -> silver -> gold -> quality -> export -> app;
  static -> dims -> gold; meteo -> gold;
}""")
    st.markdown(
        "One Databricks job (serverless, Free Edition) runs every 2 hours from 6 a.m. to 10 p.m. Eastern. "
        "Each step is idempotent: re-running it adds nothing. The dashboard snapshot is published only "
        "after the data-quality checks pass. Code: "
        "[github.com/Srinivas-Vengaldas/dc-transit-pulse](https://github.com/Srinivas-Vengaldas/dc-transit-pulse)."
    )

    m = snap.metrics
    if m:
        fr, dq, jr = m.get("freshness") or {}, m.get("data_quality") or {}, m.get("job_runs") or {}
        vp = (m.get("rows_removed") or {}).get("vehicle_positions") or {}
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric(
            "Events per day",
            f"{m['avg_events_per_full_day']:,}" if m.get("avg_events_per_full_day") else "n/a",
            help="Bronze rows added per full day (both feeds), from the run log.",
        )
        c2.metric(
            "Median freshness",
            f"{fr['median_s'] / 60:.1f} min" if fr.get("median_s") else "n/a",
            help="Newest observed departure to gold build finish, scheduled runs.",
        )
        c3.metric(
            "Positions removed (duplicates, late)",
            fmt_pct(vp.get("pct_removed")),
            help="(bronze - silver - quarantine) / bronze, vehicle positions.",
        )
        c4.metric(
            "Data-quality checks passed",
            fmt_pct(dq.get("pct_checks_passed")),
            help=f"{dq.get('passed', 0):,} of {dq.get('checks', 0):,} checks over {dq.get('runs', 0)} runs.",
        )
        c5.metric(
            "Job runs succeeded",
            fmt_pct(jr.get("pct_succeeded")),
            help=f"{jr.get('succeeded', 0)} of {jr.get('runs', 0)} scheduled runs.",
        )

    runs = snap["pipeline_runs"]
    if not runs.empty:
        left, right = st.columns(2)
        with left:
            st.markdown("**Events ingested per day**")
            epd = data.events_per_day(runs)
            chart(
                alt.Chart(epd)
                .mark_bar(color=BLUE, cornerRadiusEnd=4)
                .encode(
                    x=alt.X(
                        "yearmonthdate(date):O", title=None, axis=alt.Axis(format="%a %b %d", labelAngle=0)
                    ),
                    y=alt.Y("events:Q", title="Rows added to bronze"),
                    tooltip=[
                        alt.Tooltip("date:T", format="%a %b %d"),
                        alt.Tooltip("events:Q", format=","),
                        alt.Tooltip("runs:Q", title="Runs"),
                    ],
                )
                .properties(height=240)
            )
        with right:
            st.markdown("**Freshness at each gold build**")
            fs = data.freshness_series(runs)
            chart(
                alt.Chart(fs)
                .mark_line(point=alt.OverlayMarkDef(size=64), color=BLUE, strokeWidth=2)
                .encode(
                    x=alt.X("time:T", title=None),
                    y=alt.Y("freshness_min:Q", title="Minutes behind real time"),
                    tooltip=[
                        alt.Tooltip("time:T", format="%a %b %d %H:%M"),
                        alt.Tooltip("freshness_min:Q", title="Minutes"),
                    ],
                )
                .properties(height=240)
            )
        st.markdown("**Step runtimes** (succeeded attempts)")
        rt = data.step_runtimes(runs)
        chart(
            alt.Chart(rt)
            .mark_bar(color=BLUE, cornerRadiusEnd=4, height=14)
            .encode(
                x=alt.X("median_s:Q", title="Median seconds"),
                y=alt.Y("step:N", sort="-x", title=None),
                tooltip=[
                    "step:N",
                    alt.Tooltip("median_s:Q", title="Median s"),
                    alt.Tooltip("max_s:Q", title="Max s"),
                    "attempts:Q",
                ],
            )
            .properties(height=200)
        )
    dqc = snap["dq_checks"]
    if not dqc.empty:
        st.markdown("**Data-quality checks** (every run)")
        st.dataframe(dqc, hide_index=True, width="stretch")
