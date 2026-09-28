"""Tests for the Volume uploader using a fake Files API client."""
from __future__ import annotations

from pathlib import Path

import pytest

from producer.volume_sink import VolumeUploader


class FakeFiles:
    def __init__(self) -> None:
        self.uploaded: dict[str, bytes] = {}

    def upload(self, file_path: str, contents, *, overwrite: bool | None = None) -> None:
        assert overwrite is True
        self.uploaded[file_path] = contents.read()


def test_upload_mirrors_relative_path(tmp_path: Path) -> None:
    local = tmp_path / "vehicle_positions" / "ingest_date=2026-09-25" / "vp_1.jsonl"
    local.parent.mkdir(parents=True)
    local.write_bytes(b'{"a":1}\n')
    files = FakeFiles()

    target = VolumeUploader("/Volumes/workspace/transit/raw/landing", tmp_path, files).upload(local)

    root = "/Volumes/workspace/transit/raw/landing"
    assert target == f"{root}/vehicle_positions/ingest_date=2026-09-25/vp_1.jsonl"
    assert files.uploaded[target] == b'{"a":1}\n'


def test_rejects_non_volume_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="/Volumes/"):
        VolumeUploader("/tmp/landing", tmp_path, FakeFiles())
