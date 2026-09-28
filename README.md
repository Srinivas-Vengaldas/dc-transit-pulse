# DC Transit Pulse

A real-time lakehouse pipeline for WMATA (DC Metro) bus data: a Python producer polls
GTFS-Realtime feeds, Databricks Auto Loader ingests the raw files into a bronze Delta
table, and PySpark builds silver and gold layers for reliability analytics
(on-time performance, delay by route and hour, bus bunching).

> Status: Week 1 of 4, repository scaffold. Results and metrics will be added only once measured.

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

On Databricks (Git folder + notebook): `pipelines/static_to_delta.py` loads the GTFS static zip,
`pipelines/bronze_ingest.py` runs Auto Loader over the landing Volume. See each module's docstring.

Secrets live only in `.env` (git-ignored) locally and in Databricks secrets in the workspace.

## Data sources

- WMATA Bus GTFS-Realtime: Vehicle Positions and Trip Updates ([developer.wmata.com](https://developer.wmata.com))
- WMATA Bus GTFS static schedule

## License

MIT
