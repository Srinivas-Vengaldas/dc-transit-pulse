# DC Transit Pulse

A real-time lakehouse pipeline for WMATA (DC Metro) bus data: a Python producer polls
GTFS-Realtime feeds, Databricks Auto Loader ingests the raw files into a bronze Delta
table, and PySpark builds silver and gold layers for reliability analytics
(on-time performance, delay by route and hour, bus bunching).

**Live site: https://dc-transit-pulse-eta.vercel.app**

> Status: Week 4 of 4, dashboard and optimization. Results and metrics will be added only once measured.

## Architecture

```
WMATA GTFS-RT (protobuf) --> producer (Python, every 30 s) --> landing files
    --> Auto Loader --> bronze Delta --> silver Delta --> gold marts --> quality checks
    --> CSV snapshot --> Streamlit dashboard
WMATA GTFS static (zip) -----------------> static Delta tables (SCD Type 2 by feed_version)
```

## Repository layout

| Path | Purpose |
|---|---|
| `producer/` | Polls GTFS-RT feeds and writes raw snapshots to the landing zone |
| `pipelines/` | Databricks jobs: static load, bronze ingest, silver, gold |
| `dashboard/` | Streamlit app that reads the exported gold snapshot |
| `web/` | React showcase site (Vite, static) built from the same snapshot, for Vercel |
| `orchestration/` | The Databricks job as code and the step runner |
| `tests/` | pytest unit tests for transformations |
| `exploration/` | One-off scripts used to understand the feeds before designing schemas |
| `docs/` | Architecture diagram and design notes |

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env        # then put your real WMATA key in .env
set -a; source .env; set +a # export the variables into your shell
```

Run the producer (writes to `LANDING_DIR`, default `./data/landing`) and the tests:

```bash
python -m producer.fetch_gtfs_rt --once
python -m producer.fetch_gtfs_rt --max-polls 10
pytest
```

The Spark tests (silver, SCD Type 2) run on a local Spark + Delta session and are skipped
unless you install them (needs Java 17+): `pip install -r requirements-spark.txt`. CI runs everything.

On Databricks (Git folder + notebook), each module's docstring shows how to call it:

| Module | Builds |
|---|---|
| `pipelines/static_to_delta.py` | `static_<file>` tables from the GTFS static zip, one set per `feed_version` |
| `pipelines/bronze_ingest.py` | `bronze_vehicle_positions`, `bronze_trip_updates` with Auto Loader |
| `pipelines/silver_transform.py` | `silver_vehicle_positions`, `silver_trip_updates` (typed, deduped with a watermark and MERGE), `silver_quarantine` (rows that fail checks), plus `reconcile()` |
| `pipelines/static_silver.py` | `silver_stop_times` with GTFS times as seconds (hours can be >= 24) |
| `pipelines/dim_scd2.py` | `dim_routes`, `dim_stops` as SCD Type 2, and the view `silver_vehicle_positions_enriched` (point-in-time route join) |
| `pipelines/gold_marts.py` | `gold_timepoint_departures`, `gold_route_hour_performance` (on-time % and delay), `gold_headways`, `gold_route_hour_bunching` |
| `pipelines/weather.py` | `silver_weather_hourly` from Open-Meteo, and the view `gold_route_hour_weather` |
| `pipelines/quality.py` | Data-quality checks on silver and gold, results in `dq_results` |
| `pipelines/metrics.py` | The pipeline metrics (events per day, freshness, rows removed, DQ pass rate, runtimes) as SQL |
| `pipelines/export_dashboard.py` | Aggregated CSV snapshot of gold for the dashboard, plus `manifest.json` |
| `pipelines/optimize.py` | Liquid clustering + OPTIMIZE, with before/after benchmarks in `ops_benchmarks` |

## Scheduled job

`orchestration/workflow.json` defines one Databricks job of serverless tasks,
`collect -> bronze -> silver -> gold -> quality -> export`, plus an independent `weather` task, with retries. Every task appends a row to
`ops_run_log`, which is where events per day and data freshness are measured.

```bash
set -a; source .env; set +a
python -m orchestration.create_job --put-secret --run-now
```

The schedule is created paused; unpause it in the Jobs UI once a manual run succeeds.

### Metric definitions

| Metric | Definition |
|---|---|
| Observed departure | Midpoint of the last ping at or before a stop and the first ping past it; graded only if the two pings are at most 120 s apart |
| On time | From 2 minutes early to 7 minutes late against the scheduled departure, at timepoints |
| Bunched / gapped | Actual headway below 25% / above 150% of the scheduled gap between the same two trips |

Secrets live only in `.env` (git-ignored) locally and in Databricks secrets in the workspace.

## Dashboard

`dashboard/app.py` is a Streamlit app that tells the story in six tabs: the problem (with a real
bunching example), which routes, when, where (map of timepoints), rain, and how the pipeline is built.

It reads a **snapshot**, not a live connection. The `export` job task writes small aggregated CSVs
and a `manifest.json` to the Volume after the quality checks pass; `fetch_snapshot` copies them into
`dashboard/snapshot/`. A live query from a public app would need a running SQL warehouse and a
token stored in the app; the snapshot costs nothing to view and holds no credentials.

```bash
set -a; source .env; set +a
python -m dashboard.fetch_snapshot
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

To publish it, commit `dashboard/snapshot/` and deploy `dashboard/app.py` on
[Streamlit Community Cloud](https://streamlit.io/cloud) (free, public URL).

### Showcase site (React, Vercel)

`web/` is a one-page static site (React + TypeScript, Recharts, Leaflet) that tells the same story for
someone who will not open a notebook. At build time it copies `dashboard/snapshot/` into the bundle, so
it is plain HTML, JS and CSV: free to host, no backend, no credentials.

```bash
cd web
npm install
npm run dev          # http://localhost:5173
npm test && npm run build
```

Deploy on [Vercel](https://vercel.com) (Hobby plan, free): import the GitHub repo, set **Root Directory**
to `web` (`web/vercel.json` pins the Vite build, because the repo's Python files would otherwise be
detected as a Python app), and optionally set `VITE_STREAMLIT_URL` to link the Streamlit app. Every
push to `main` that updates the snapshot redeploys the site.

`.github/workflows/refresh-snapshot.yml` keeps it current: every night at 03:30 UTC (after the last
job run) it downloads the published snapshot and commits it if it changed, which redeploys the site.
It needs repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`; run it by hand from the Actions tab.

## Data sources

- WMATA Bus GTFS-Realtime: Vehicle Positions and Trip Updates ([developer.wmata.com](https://developer.wmata.com))
- WMATA Bus GTFS static schedule

## License

MIT
