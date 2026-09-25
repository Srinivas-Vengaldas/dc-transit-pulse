"""Compare two saved Vehicle Positions snapshots to measure repeat pings.

Answers: if we poll every N seconds, how many (vehicle_id, timestamp) pairs
are exact repeats of the previous poll? That tells us how much dedupe matters
and whether our poll interval is too fast or too slow.

Usage:
    python compare_snapshots.py samples/<older>.pb samples/<newer>.pb
"""
from __future__ import annotations

import sys
from pathlib import Path

from google.transit import gtfs_realtime_pb2


def load_keys(path: Path) -> tuple[int, set[tuple[str, int]]]:
    """Return (header_timestamp, set of (vehicle_id, vehicle_timestamp))."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(path.read_bytes())
    keys = {(e.vehicle.vehicle.id, e.vehicle.timestamp)
            for e in feed.entity if e.HasField("vehicle")}
    return feed.header.timestamp, keys


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    t1, a = load_keys(Path(sys.argv[1]))
    t2, b = load_keys(Path(sys.argv[2]))
    repeats = a & b
    print(f"gap between snapshots: {t2 - t1}s")
    print(f"pings in older: {len(a)}  newer: {len(b)}")
    print(f"exact repeats (same vehicle, same timestamp): {len(repeats)}"
          f" = {len(repeats) / len(b):.1%} of newer")
    print(f"vehicles only in older: {len({v for v, _ in a} - {v for v, _ in b})}"
          f"  only in newer: {len({v for v, _ in b} - {v for v, _ in a})}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
