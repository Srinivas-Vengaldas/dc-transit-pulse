"""Fetch one WMATA GTFS-Realtime snapshot and summarize it.

Week 1 exploration script: not the producer. It fetches a single feed
snapshot, saves the raw protobuf bytes, and prints what is inside so we can
decide on the bronze schema before writing any pipeline code.

Usage:
    export WMATA_API_KEY=...        # never hard-code the key
    python explore_feed.py <feed_url>

Copy <feed_url> from the "Request" line of the endpoint in the WMATA portal,
e.g. Bus RT Vehicle Positions.
"""
from __future__ import annotations

import logging
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("explore_feed")


def fetch_feed(url: str, api_key: str, timeout_s: int = 20) -> bytes:
    """Download one GTFS-RT snapshot and return the raw protobuf bytes."""
    resp = requests.get(url, headers={"api_key": api_key}, timeout=timeout_s)
    resp.raise_for_status()  # 401 = bad key, 429 = rate limited
    return resp.content


def parse_feed(raw: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Decode protobuf bytes into a FeedMessage object."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)
    return feed


def summarize(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    """Print header, entity counts, feed age, and two sample entities."""
    header_ts = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)
    age_s = (datetime.now(timezone.utc) - header_ts).total_seconds()
    print(f"gtfs_realtime_version: {feed.header.gtfs_realtime_version}")
    print(f"header timestamp (UTC): {header_ts.isoformat()}  (age {age_s:.0f}s)")
    print(f"entities: {len(feed.entity)}")

    kinds = Counter(
        "vehicle" if e.HasField("vehicle")
        else "trip_update" if e.HasField("trip_update")
        else "alert" if e.HasField("alert") else "other"
        for e in feed.entity
    )
    print(f"entity types: {dict(kinds)}")

    for e in feed.entity[:2]:
        print("---- sample entity ----")
        print(MessageToDict(e, preserving_proto_field_name=True))


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    api_key = os.environ.get("WMATA_API_KEY")
    if not api_key:
        log.error("WMATA_API_KEY is not set")
        return 1

    url = sys.argv[1]
    raw = fetch_feed(url, api_key)
    log.info("downloaded %d bytes from %s", len(raw), url)

    # Keep the raw bytes: we can replay/re-parse later without hitting the API.
    out = Path("samples") / f"{Path(url).stem}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.pb"
    out.parent.mkdir(exist_ok=True)
    out.write_bytes(raw)
    log.info("saved raw snapshot to %s", out)

    summarize(parse_feed(raw))
    return 0


if __name__ == "__main__":
    sys.exit(main())
