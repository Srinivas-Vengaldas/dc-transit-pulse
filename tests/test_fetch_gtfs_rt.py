"""Unit tests for the producer. No network: feeds are built in memory."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests
from google.transit import gtfs_realtime_pb2

from producer import fetch_gtfs_rt as p

HEADER_TS = 1790370484  # 2026-09-25 UTC


def make_feed(vehicle_ids: list[str]) -> gtfs_realtime_pb2.FeedMessage:
    """Build a small Vehicle Positions feed like the ones WMATA returns."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = HEADER_TS
    for vid in vehicle_ids:
        e = feed.entity.add()
        e.id = vid
        e.vehicle.vehicle.id = vid
        e.vehicle.timestamp = HEADER_TS - 5
        e.vehicle.position.latitude = 38.9
        e.vehicle.position.longitude = -77.0
    return feed


def test_to_records_one_per_entity_with_metadata() -> None:
    fetched = datetime(2026, 9, 25, 21, 0, tzinfo=UTC)
    recs = list(p.to_records(make_feed(["a", "b"]), "vehicle_positions", fetched))

    assert len(recs) == 2
    assert recs[0]["feed"] == "vehicle_positions"
    assert recs[0]["feed_header_ts"] == HEADER_TS
    assert recs[0]["fetched_at"] == "2026-09-25T21:00:00+00:00"
    assert recs[0]["entity"]["vehicle"]["vehicle"]["id"] == "a"
    # protobuf JSON renders uint64 as a string; silver will cast it
    assert recs[0]["entity"]["vehicle"]["timestamp"] == str(HEADER_TS - 5)


def test_landing_path_partitions_by_utc_date(tmp_path: Path) -> None:
    path = p.landing_path(tmp_path, "trip_updates", HEADER_TS)
    assert path == tmp_path / "trip_updates" / "ingest_date=2026-09-25" / f"trip_updates_{HEADER_TS}.jsonl"


def test_write_atomically_leaves_no_temp_file(tmp_path: Path) -> None:
    target = tmp_path / "x" / "snap.jsonl"
    n = p.write_atomically(target, iter([{"a": 1}, {"a": 2}]))

    assert n == 2
    assert [json.loads(line) for line in target.read_text().splitlines()] == [{"a": 1}, {"a": 2}]
    assert list(target.parent.iterdir()) == [target]


class FakeResponse:
    def __init__(self, status: int, content: bytes = b"") -> None:
        self.status_code = status
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """Returns queued responses in order and counts calls."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls = 0

    def get(self, *args: object, **kwargs: object) -> FakeResponse:
        self.calls += 1
        return self.responses.pop(0)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(p.time, "sleep", lambda s: None)


def test_fetch_retries_on_429_then_succeeds() -> None:
    session = FakeSession([FakeResponse(429), FakeResponse(200, b"ok")])
    assert p.fetch_snapshot(session, "u", "k") == b"ok"  # type: ignore[arg-type]
    assert session.calls == 2


def test_fetch_does_not_retry_bad_key() -> None:
    session = FakeSession([FakeResponse(401)])
    with pytest.raises(requests.HTTPError):
        p.fetch_snapshot(session, "u", "k")  # type: ignore[arg-type]
    assert session.calls == 1


def test_fetch_gives_up_after_max_attempts() -> None:
    session = FakeSession([FakeResponse(503)] * 3)
    with pytest.raises(RuntimeError, match="after 3 attempts"):
        p.fetch_snapshot(session, "u", "k", max_attempts=3)  # type: ignore[arg-type]


def test_poll_once_skips_snapshot_already_landed(tmp_path: Path) -> None:
    raw = make_feed(["a"]).SerializeToString()
    cfg = p.Config(api_key="k", feeds={"vehicle_positions": "u"}, landing_dir=tmp_path, poll_interval_s=30)

    first = p.poll_once(FakeSession([FakeResponse(200, raw)]), cfg)  # type: ignore[arg-type]
    second = p.poll_once(FakeSession([FakeResponse(200, raw)]), cfg)  # type: ignore[arg-type]

    assert first == {"vehicle_positions": 1}
    assert second == {"vehicle_positions": 0}
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1


def test_poll_once_upload_failure_keeps_local_file(tmp_path: Path) -> None:
    class BrokenUploader:
        def upload(self, path: Path) -> str:
            raise ConnectionError("network down")

    raw = make_feed(["a"]).SerializeToString()
    cfg = p.Config(api_key="k", feeds={"vehicle_positions": "u"}, landing_dir=tmp_path, poll_interval_s=30)
    written = p.poll_once(FakeSession([FakeResponse(200, raw)]), cfg, BrokenUploader())  # type: ignore[arg-type]

    assert written == {"vehicle_positions": 1}
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1
