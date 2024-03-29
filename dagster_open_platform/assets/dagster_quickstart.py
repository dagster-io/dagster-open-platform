import subprocess
import tempfile

import requests
from dagster import (
    AssetExecutionContext,
    Failure,
    RunRequest,
    SensorEvaluationContext,
    asset,
    define_asset_job,
    sensor,
)

DAGSTER_QUICKSTART_REPO = "dagster-io/dagster-quickstart"


@asset(group_name="validation")
def dagster_quickstart_validation(context: AssetExecutionContext):
    with tempfile.TemporaryDirectory() as tmp:
        context.log.info("Using temporary directory %s", tmp)

        commands = [
            f"git clone --depth 1 https://github.com/{DAGSTER_QUICKSTART_REPO} {tmp}/dagster-quickstart",
            f"cd {tmp}/dagster-quickstart",
            "python3 -m venv venv",
            "source venv/bin/activate",
            "pip install -e '.[dev]'",
            "pytest",
        ]

        process = subprocess.run(
            ["bash", "-c", ";".join(commands)], capture_output=True, check=False
        )
        context.log.info(process.stdout.decode("utf-8"))

    if process.returncode != 0:
        stderr = process.stderr.decode("utf-8")
        context.log.error(stderr)
        raise Failure(
            description="Quickstart validation failure",
            metadata={"stderr": stderr},
        )


def _get_most_recent_sha(repo: str):
    """Returns the most recent Git sha from a GitHub repo.

    Args:
        repo (str): <owner>/<repo> on GitHub (eg. dagster-io/dagster-quickstart)
    """
    response = requests.get(
        f"https://api.github.com/repos/{repo}/commits",
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    assert response.status_code == 200
    return response.json()[0]["sha"]


dagster_quickstart_validation_job = define_asset_job(
    name="dagster_quickstart_validation_job",
    selection=[dagster_quickstart_validation],
    tags={"team": "devrel"},
)


@sensor(job=dagster_quickstart_validation_job, minimum_interval_seconds=10 * 60)
def dagster_quickstart_validation_sensor(context: SensorEvaluationContext):
    """Sensor to poll if a Git commit has been made to dagster-io/dagster-quickstart."""
    previous_git_commit_sha = context.cursor

    current_git_commit_sha = _get_most_recent_sha(DAGSTER_QUICKSTART_REPO)

    if current_git_commit_sha != previous_git_commit_sha:
        yield RunRequest(
            run_key=f"dagster_quickstart_validation_{current_git_commit_sha}",
        )
        context.update_cursor(str(current_git_commit_sha))
