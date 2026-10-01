"""Create or update the scheduled Databricks job from orchestration/workflow.json.

Runs on your laptop with the same .env as the producer (DATABRICKS_HOST and
DATABRICKS_TOKEN; the SDK reads them from the environment). Idempotent: the job
is found by name and updated in place, so running this twice never makes two jobs.

    set -a; source .env; set +a
    python -m orchestration.create_job --put-secret --run-now

--repo-path defaults to /Workspace/Users/<your Databricks user>/dc-transit-pulse, looked up
from the token, so no email is typed by hand.

--put-secret copies WMATA_API_KEY from your environment into the Databricks
secret scope the collect task reads, so the key never appears in the job or the repo.
The schedule is created PAUSED: unpause it in the Jobs UI after a manual run succeeds.
Later updates keep whatever pause state the job has.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

log = logging.getLogger("create_job")

WORKFLOW = Path(__file__).with_name("workflow.json")


def job_settings(repo_path: str, workflow: Path = WORKFLOW) -> dict[str, Any]:
    """workflow.json with {repo} replaced by the workspace path of the Git folder."""
    if not repo_path.startswith("/Workspace/"):
        raise ValueError(f"repo path must start with /Workspace/, got {repo_path!r}")
    return json.loads(workflow.read_text().replace("{repo}", repo_path.rstrip("/")))


def default_repo_path(user_name: str) -> str:
    """Where Databricks puts a Git folder named dc-transit-pulse in the user's home folder."""
    return f"/Workspace/Users/{user_name}/dc-transit-pulse"


def put_secret(w: Any, scope: str, key: str, value: str) -> None:
    """Create the secret scope if needed and store the value. The value is never logged."""
    if scope not in {s.name for s in w.secrets.list_scopes()}:
        w.secrets.create_scope(scope=scope)
        log.info("created secret scope %s", scope)
    w.secrets.put_secret(scope=scope, key=key, string_value=value)
    log.info("stored secret %s/%s", scope, key)


def upsert_job(w: Any, settings: dict[str, Any]) -> int:
    """Create the job, or reset the existing job with the same name to these settings. Returns job_id."""
    existing = [j.job_id for j in w.jobs.list(name=settings["name"])]
    if len(existing) > 1:
        raise RuntimeError(f"{len(existing)} jobs named {settings['name']!r}; delete the extras first")
    if existing:
        # Keep the schedule paused or unpaused as it is now: updating the code must not switch it.
        current = w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": existing[0]})
        status = current.get("settings", {}).get("schedule", {}).get("pause_status")
        if status and "schedule" in settings:
            settings = {**settings, "schedule": {**settings["schedule"], "pause_status": status}}
        w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": existing[0], "new_settings": settings})
        log.info("updated job %s", existing[0])
        return existing[0]
    job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
    log.info("created job %s", job_id)
    return job_id


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--repo-path", help="Git folder path (default: /Workspace/Users/<you>/dc-transit-pulse)")
    p.add_argument("--put-secret", action="store_true", help="store WMATA_API_KEY in the secret scope")
    p.add_argument("--run-now", action="store_true", help="start one run after creating or updating")
    p.add_argument("--secret-scope", default="transit")
    p.add_argument("--secret-key", default="wmata_api_key")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    if args.put_secret:
        key = os.environ.get("WMATA_API_KEY", "")
        if not key:
            log.error("WMATA_API_KEY is not set (did you run: set -a; source .env; set +a ?)")
            return 1
        put_secret(w, args.secret_scope, args.secret_key, key)
    repo_path = args.repo_path or default_repo_path(w.current_user.me().user_name)
    job_id = upsert_job(w, job_settings(repo_path))
    host = w.config.host.rstrip("/")
    log.info("job page: %s/jobs/%s", host, job_id)
    if args.run_now:
        run = w.jobs.run_now(job_id=job_id)
        log.info("started run %s: %s/jobs/%s/runs/%s", run.run_id, host, job_id, run.run_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
