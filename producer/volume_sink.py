"""Copy landed snapshot files to a Unity Catalog Volume so Auto Loader can read them.

Uses the Databricks Files API through the official SDK. The SDK reads
DATABRICKS_HOST and DATABRICKS_TOKEN from the environment, so no credentials
appear in code.
"""
from __future__ import annotations

import logging
from pathlib import Path, PurePosixPath
from typing import Protocol

log = logging.getLogger("volume_sink")


class FilesClient(Protocol):
    """The one method we need from databricks.sdk's WorkspaceClient().files."""

    def upload(self, file_path: str, contents, *, overwrite: bool | None = None) -> None: ...


class VolumeUploader:
    """Uploads local files under local_root to the same relative path under volume_root."""

    def __init__(self, volume_root: str, local_root: Path, files: FilesClient | None = None) -> None:
        if not volume_root.startswith("/Volumes/"):
            raise ValueError(f"volume_root must start with /Volumes/, got {volume_root!r}")
        self.volume_root = PurePosixPath(volume_root)
        self.local_root = local_root
        if files is None:
            from databricks.sdk import WorkspaceClient  # imported lazily: optional dependency

            files = WorkspaceClient().files
        self.files = files

    def target_for(self, local_path: Path) -> str:
        """Map data/landing/<feed>/ingest_date=.../x.jsonl -> /Volumes/.../<feed>/ingest_date=.../x.jsonl."""
        rel = local_path.relative_to(self.local_root)
        return str(self.volume_root.joinpath(*rel.parts))

    def upload(self, local_path: Path) -> str:
        """Upload one file. The object only becomes visible once the upload completes,
        so Auto Loader never reads a partial file. Returns the Volume path."""
        target = self.target_for(local_path)
        with local_path.open("rb") as fh:
            # overwrite=True keeps retries idempotent: same snapshot, same path, same bytes.
            self.files.upload(target, fh, overwrite=True)
        return target
