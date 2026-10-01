"""Download the latest dashboard snapshot from the Databricks Volume into dashboard/snapshot/.

The `export` job task writes the CSVs and manifest.json to the Volume. This
script copies them to the repo so the Streamlit app (locally or on Streamlit
Community Cloud) reads plain files and never needs Databricks credentials.

Credentials come from DATABRICKS_HOST and DATABRICKS_TOKEN in the environment
(load .env first). Run from the repo root:

    set -a; source .env; set +a
    python -m dashboard.fetch_snapshot
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Protocol

from dashboard.data import DATASETS, DEFAULT_DIR

log = logging.getLogger("fetch_snapshot")

DEFAULT_SOURCE = "/Volumes/workspace/transit/raw/exports/dashboard"
FILES = ("manifest.json", *(f"{name}.csv" for name in DATASETS))


class FilesClient(Protocol):
    """The one method we need from databricks.sdk's WorkspaceClient().files."""

    def download(self, file_path: str): ...


def fetch(files: FilesClient, source: str, target: Path) -> dict[str, int]:
    """Copy every snapshot file that exists in `source` to `target`. Returns bytes per file.

    Each file lands under a temp name and is renamed, and manifest.json goes last,
    so an interrupted fetch never leaves a manifest that describes CSVs it does not have.
    """
    target.mkdir(parents=True, exist_ok=True)
    sizes: dict[str, int] = {}
    for name in sorted(FILES, key=lambda n: n == "manifest.json"):
        try:
            body = files.download(f"{source.rstrip('/')}/{name}").contents.read()
        except Exception as exc:  # a dataset the export skipped is simply absent
            if name == "manifest.json":
                raise
            log.warning("skipped %s: %s", name, exc)
            continue
        tmp = target / f".{name}.tmp"
        tmp.write_bytes(body)
        os.replace(tmp, target / name)
        sizes[name] = len(body)
    return sizes


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--source", default=DEFAULT_SOURCE, help="Volume folder the export task writes to")
    p.add_argument("--target", type=Path, default=DEFAULT_DIR)
    args = p.parse_args(argv)
    from databricks.sdk import WorkspaceClient  # reads DATABRICKS_HOST / DATABRICKS_TOKEN

    sizes = fetch(WorkspaceClient().files, args.source, args.target)
    for name, n in sizes.items():
        log.info("%-24s %8d bytes", name, n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
