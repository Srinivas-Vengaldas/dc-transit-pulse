"""Download the WMATA bus GTFS static zip to the local data folder.

The static feed (routes, stops, trips, stop_times...) changes every few months,
so this runs on demand, not every 30 s. The saved zip is then uploaded to a
Databricks Volume and loaded to Delta by pipelines/static_to_delta.py.

Usage:
    set -a; source .env; set +a
    python -m producer.download_gtfs_static
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import requests

from pipelines.static_to_delta import read_feed_version
from producer.fetch_gtfs_rt import fetch_snapshot

log = logging.getLogger("download_static")


def main() -> int:
    """Download the zip, name it by feed_version, and report where it landed."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    api_key = os.environ.get("WMATA_API_KEY", "")
    url = os.environ.get("WMATA_BUS_STATIC_URL", "")
    if not api_key or not url:
        log.error("WMATA_API_KEY and WMATA_BUS_STATIC_URL must be set")
        return 1

    out_dir = Path(os.environ.get("STATIC_DIR", "./data/static"))
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out_dir / ".download.zip.tmp"

    with requests.Session() as session:
        tmp.write_bytes(fetch_snapshot(session, url, api_key, timeout_s=120))

    # Name the file by the feed's own version so re-downloads of the same
    # schedule overwrite one file instead of piling up copies.
    version = read_feed_version(tmp)
    final = out_dir / f"bus_gtfs_static_{version}.zip"
    os.replace(tmp, final)
    log.info("saved %s (%.1f MB), feed_version=%s", final, final.stat().st_size / 1e6, version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
