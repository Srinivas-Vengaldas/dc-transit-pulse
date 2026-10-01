"""Tests for the job definition and the pipeline step runner (no Databricks needed)."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestration import run_pipeline
from orchestration.create_job import WORKFLOW, default_repo_path, job_settings, upsert_job

REPO = "/Workspace/Users/someone/dc-transit-pulse"


def test_job_runs_steps_in_order_with_retries() -> None:
    s = job_settings(REPO)
    tasks = {t["task_key"]: t for t in s["tasks"]}
    assert list(tasks) == ["collect", "bronze", "silver", "gold"]
    assert tasks["silver"]["depends_on"] == [{"task_key": "bronze"}]
    assert tasks["gold"]["depends_on"] == [{"task_key": "silver"}]
    # Bronze still runs if collect fails: files that did land are not left waiting.
    assert tasks["bronze"]["run_if"] == "ALL_DONE"
    for t in tasks.values():
        assert t["max_retries"] >= 1
        assert t["spark_python_task"]["python_file"] == f"{REPO}/orchestration/run_pipeline.py"
        assert t["spark_python_task"]["parameters"][:2] == ["--step", t["task_key"]]
    assert s["max_concurrent_runs"] == 1
    assert s["schedule"]["pause_status"] == "PAUSED"  # nothing runs (or costs) until unpaused


def test_job_definition_holds_no_secrets() -> None:
    text = WORKFLOW.read_text().lower()
    assert "api_key" not in text and "token" not in text and "dapi" not in text


def test_job_settings_rejects_non_workspace_path() -> None:
    with pytest.raises(ValueError):
        job_settings("~/dc-transit-pulse")


class FakeWorkspace:
    def __init__(self, existing: list[int]) -> None:
        self.calls: list[tuple] = []
        self.jobs = SimpleNamespace(list=lambda name: [SimpleNamespace(job_id=j) for j in existing])
        self.api_client = SimpleNamespace(do=self._do)

    def _do(self, method: str, path: str, body: dict) -> dict:
        self.calls.append((method, path, body))
        return {"job_id": 42}


def test_upsert_creates_then_updates_in_place() -> None:
    settings = json.loads(Path(WORKFLOW).read_text())
    new = FakeWorkspace(existing=[])
    assert upsert_job(new, settings) == 42 and new.calls[0][1] == "/api/2.2/jobs/create"
    old = FakeWorkspace(existing=[7])
    assert upsert_job(old, settings) == 7
    assert old.calls[0][1] == "/api/2.2/jobs/reset" and old.calls[0][2]["job_id"] == 7


def test_upsert_refuses_duplicate_names() -> None:
    with pytest.raises(RuntimeError):
        upsert_job(FakeWorkspace(existing=[1, 2]), {"name": "dc-transit-pulse"})


def test_collect_sums_polls_and_fails_when_nothing_lands(monkeypatch, tmp_path: Path) -> None:
    import producer.fetch_gtfs_rt as producer

    results = iter([{"vehicle_positions": 1000, "trip_updates": 1900},
                    {"vehicle_positions": 0, "trip_updates": 1950}])
    monkeypatch.setattr(producer, "poll_once", lambda session, cfg: next(results))
    out = run_pipeline.collect(str(tmp_path), polls=2, api_key="k", interval_s=0)
    assert out == {"vehicle_positions_records": 1000, "trip_updates_records": 3850}

    nothing = {"vehicle_positions": 0, "trip_updates": 0}
    monkeypatch.setattr(producer, "poll_once", lambda session, cfg: nothing)
    with pytest.raises(RuntimeError):
        run_pipeline.collect(str(tmp_path), polls=1, api_key="k", interval_s=0)


def test_parse_args_defaults() -> None:
    a = run_pipeline.parse_args(["--step", "gold"])
    assert (a.step, a.run_id, a.raw_volume) == ("gold", "manual", "/Volumes/workspace/transit/raw")


def test_default_repo_path_is_the_users_git_folder() -> None:
    assert default_repo_path("a@b.edu") == "/Workspace/Users/a@b.edu/dc-transit-pulse"
    assert job_settings(default_repo_path("a@b.edu"))["tasks"][0]["spark_python_task"]["python_file"] == (
        "/Workspace/Users/a@b.edu/dc-transit-pulse/orchestration/run_pipeline.py"
    )
