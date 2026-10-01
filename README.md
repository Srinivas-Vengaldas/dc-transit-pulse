# DC Transit Pulse

A real-time lakehouse pipeline for WMATA (DC Metro) bus data: a Python producer polls
GTFS-Realtime feeds, Databricks Auto Loader ingests the raw files into a bronze Delta
table, and PySpark builds silver and gold layers for reliability analytics
(on-time performance, delay by route and hour, bus bunching).

> Status: Week 3 of 4, gold marts and scheduled job. Results and metrics will be added only once measured.

## Architecture

```
WMATA GTFS-RT (protobuf) --> producer (Python, every 30 s) --> landing files
    --> Auto Loader --> bronze Delta --> silver Delta --> gold marts --> dashboard
WMATA GTFS static (zip) -----------------> static Delta tables (SCD Type 2 by feed_version)
```

## Repository layout

| Path | Purpose |
|---|---|
| `producer/` | Polls GTFS-RT feeds and writes raw snapshots to the landing zone |
| `pipelines/` | Databricks jobs: static load, bronze ingest, silver, gold |
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

## Scheduled job

`orchestration/workflow.json` defines one Databricks job with four serverless tasks,
`collect -> bronze -> silver -> gold`, each with retries. Every task appends a row to
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

## Data sources

- WMATA Bus GTFS-Realtime: Vehicle Positions and Trip Updates ([developer.wmata.com](https://developer.wmata.com))
- WMATA Bus GTFS static schedule

## License

MIT
