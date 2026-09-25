"""Poll WMATA bus GTFS-Realtime feeds and land each snapshot as a JSON Lines file.

This is the producer at the front of the pipeline:

    WMATA GTFS-RT (protobuf) --> this script --> landing dir --> Auto Loader --> bronze

Each poll downloads one snapshot per feed, decodes the protobuf, and writes one
file with one JSON object per entity (vehicle or trip update). The landing
layout is:

    <LANDING_DIR>/<feed>/ingest_date=YYYY-MM-DD/<feed>_<header_ts>.jsonl

Usage:
    set -a; source .env; set +a
    python -m producer.fetch_gtfs_rt --once          # one poll of each feed, then exit
    python -m producer.fetch_gtfs_rt --max-polls 10  # ten polls, POLL_INTERVAL_SECONDS apart
    python -m producer.fetch_gtfs_rt                 # run until Ctrl+C
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

log = logging.getLogger("producer")

# HTTP statuses worth retrying: rate limiting and transient server errors.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class Config:
    """Producer settings, read from environment variables (never hard-coded)."""

    api_key: str
    feeds: dict[str, str]  # feed name -> URL
    landing_dir: Path
    poll_interval_s: int

    @classmethod
    def from_env(cls) -> Config:
        """Build a Config from the environment; fail fast if the key is missing."""
        api_key = os.environ.get("WMATA_API_KEY", "")
        if not api_key:
            raise ValueError("WMATA_API_KEY is not set (did you run: set -a; source .env; set +a ?)")
        return cls(
            api_key=api_key,
            feeds={
                "vehicle_positions": os.environ["WMATA_VEHICLE_POSITIONS_URL"],
                "trip_updates": os.environ["WMATA_TRIP_UPDATES_URL"],
            },
            landing_dir=Path(os.environ.get("LANDING_DIR", "./data/landing")),
            poll_interval_s=int(os.environ.get("POLL_INTERVAL_SECONDS", "30")),
        )


def fetch_snapshot(
    session: requests.Session,
    url: str,
    api_key: str,
    max_attempts: int = 4,
    timeout_s: float = 20.0,
) -> bytes:
    """Download one feed snapshot, retrying transient failures with exponential backoff.

    Retries on network errors and on 429/5xx. A 401/403 is not retried: a bad key
    will not fix itself, and hammering the API with it wastes quota.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, headers={"api_key": api_key}, timeout=timeout_s)
            if resp.status_code not in RETRYABLE_STATUS:
                resp.raise_for_status()
                return resp.content
            reason = f"HTTP {resp.status_code}"
        except (requests.ConnectionError, requests.Timeout) as exc:
            reason = type(exc).__name__
        if attempt == max_attempts:
            raise RuntimeError(f"giving up on {url} after {attempt} attempts ({reason})")
        wait_s = 2**attempt  # 2, 4, 8 s
        log.warning("attempt %d/%d failed (%s); retrying in %ds", attempt, max_attempts, reason, wait_s)
        time.sleep(wait_s)
    raise AssertionError("unreachable")


def parse_feed(raw: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Decode GTFS-RT protobuf bytes into a FeedMessage."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)
    return feed


def to_records(
    feed: gtfs_realtime_pb2.FeedMessage, feed_name: str, fetched_at: datetime
) -> Iterator[dict]:
    """Flatten a feed into one dict per entity, tagged with snapshot metadata.

    The entity body is kept exactly as GTFS-RT defines it (bronze stays raw);
    parsing and type casting happen later in silver. Note that MessageToDict
    renders int64 fields such as `timestamp` as strings, per the protobuf JSON spec.
    """
    for entity in feed.entity:
        yield {
            "feed": feed_name,
            "feed_header_ts": feed.header.timestamp,
            "fetched_at": fetched_at.isoformat(),
            "entity": MessageToDict(entity, preserving_proto_field_name=True),
        }


def landing_path(landing_dir: Path, feed_name: str, header_ts: int) -> Path:
    """Where a snapshot lands. The header timestamp in the name makes writes idempotent."""
    day = datetime.fromtimestamp(header_ts, tz=UTC).strftime("%Y-%m-%d")
    return landing_dir / feed_name / f"ingest_date={day}" / f"{feed_name}_{header_ts}.jsonl"


def write_atomically(path: Path, records: Iterator[dict]) -> int:
    """Write JSON Lines to a hidden temp file, then rename it into place.

    Spark and Auto Loader skip files whose names start with '.', and a rename on
    the same filesystem is atomic, so a reader never sees a half-written file.
    Returns the number of records written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    n = 0
    with tmp.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
            n += 1
    os.replace(tmp, path)
    return n


def poll_once(session: requests.Session, cfg: Config) -> dict[str, int]:
    """Fetch every feed once and land new snapshots. Returns records written per feed."""
    written: dict[str, int] = {}
    for feed_name, url in cfg.feeds.items():
        try:
            raw = fetch_snapshot(session, url, cfg.api_key)
        except (RuntimeError, requests.HTTPError) as exc:
            log.error("%s: fetch failed: %s", feed_name, exc)
            written[feed_name] = 0
            continue
        fetched_at = datetime.now(UTC)
        feed = parse_feed(raw)
        path = landing_path(cfg.landing_dir, feed_name, feed.header.timestamp)
        if path.exists():
            # WMATA has not refreshed this feed since our last poll: same snapshot, skip it.
            log.info("%s: snapshot %d already landed, skipping", feed_name, feed.header.timestamp)
            written[feed_name] = 0
            continue
        n = write_atomically(path, to_records(feed, feed_name, fetched_at))
        age_s = fetched_at.timestamp() - feed.header.timestamp
        log.info("%s: %d records, %d bytes, feed age %.0fs -> %s", feed_name, n, len(raw), age_s, path)
        written[feed_name] = n
    return written


def run(cfg: Config, max_polls: int | None) -> None:
    """Poll on a fixed interval until max_polls is reached or Ctrl+C."""
    polls = 0
    with requests.Session() as session:
        while max_polls is None or polls < max_polls:
            started = time.monotonic()
            poll_once(session, cfg)
            polls += 1
            if max_polls is not None and polls >= max_polls:
                break
            # Sleep the rest of the interval so polls stay on a steady cadence.
            time.sleep(max(0.0, cfg.poll_interval_s - (time.monotonic() - started)))
    log.info("stopped after %d polls", polls)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--once", action="store_true", help="poll each feed once and exit")
    group.add_argument("--max-polls", type=int, help="stop after this many polls")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        cfg = Config.from_env()
    except (ValueError, KeyError) as exc:
        log.error("config error: %s", exc)
        return 1

    try:
        run(cfg, max_polls=1 if args.once else args.max_polls)
    except KeyboardInterrupt:
        log.info("interrupted, exiting cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
