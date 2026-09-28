"""Unit tests for the pure-Python parts of the static loader (no Spark needed)."""
from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from pipelines.static_to_delta import extract_gtfs, read_feed_version


def make_zip(path: Path, files: dict[str, str]) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)
    return path


FEED_INFO = "feed_publisher_name,feed_version\nWMATA,S1000251\n"


def test_read_feed_version(tmp_path: Path) -> None:
    z = make_zip(tmp_path / "g.zip", {"feed_info.txt": FEED_INFO})
    assert read_feed_version(z) == "S1000251"


def test_read_feed_version_handles_byte_order_mark(tmp_path: Path) -> None:
    z = make_zip(tmp_path / "g.zip", {"feed_info.txt": "﻿" + FEED_INFO})
    assert read_feed_version(z) == "S1000251"


def test_read_feed_version_missing_value_fails(tmp_path: Path) -> None:
    z = make_zip(tmp_path / "g.zip", {"feed_info.txt": "feed_publisher_name,feed_version\nWMATA,\n"})
    with pytest.raises(ValueError, match="no feed_version"):
        read_feed_version(z)


def test_extract_keeps_only_txt_files(tmp_path: Path) -> None:
    z = make_zip(
        tmp_path / "g.zip",
        {"routes.txt": "route_id\nA1\n", "stops.txt": "stop_id\n1\n", "README.pdf": "x"},
    )
    names = extract_gtfs(z, tmp_path / "out")
    assert names == ["routes", "stops"]
    assert (tmp_path / "out" / "routes.txt").read_text() == "route_id\nA1\n"
    assert not (tmp_path / "out" / "README.pdf").exists()


def test_extract_blocks_zip_slip(tmp_path: Path) -> None:
    z = make_zip(tmp_path / "g.zip", {"../../evil.txt": "x"})
    extract_gtfs(z, tmp_path / "out")
    assert (tmp_path / "out" / "evil.txt").exists()
    assert not (tmp_path.parent / "evil.txt").exists()
