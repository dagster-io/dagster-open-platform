import datetime
import os
from typing import Any, Optional, cast

import requests
from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor
from dagster_snowflake import SnowflakeResource


def _query_gh_api(method: str, endpoint: str, data: Optional[Any] = None) -> Any:
    """Queries the GitHub API with the given method and endpoint, and returns the JSON response."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {os.getenv('GITHUB_ACCESS_TOKEN')}",
    }

    url = f"https://api.github.com/{endpoint}"
    return requests.request(method=method, url=url, headers=headers, json=data).json()


PREFIX = "PURINA_CLONE_"


@sensor(job_name="drop_database_clones", minimum_interval_seconds=86400)
def drop_old_database_clones(ctx: SensorEvaluationContext, snowflake_telemetry: SnowflakeResource):
    # fetch list Snowflake DBs with prefix "PURINA_CLONE_"
    with snowflake_telemetry.get_connection() as conn:
        databases = cast(
            list[tuple[str, ...]],
            conn.cursor().execute(
                "SELECT database_name FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES",
            ),
        )
    pr_ids_snowflake_clones = [db[0][len(PREFIX) :] for db in databases if db[0].startswith(PREFIX)]

    result: list[dict[str, Any]] = _query_gh_api(
        "GET",
        "repos/dagster-io/internal/pulls?state=closed&sort=updated&direction=desc&per_page=100",
    )

    last_cursor_time = (
        datetime.datetime.fromisoformat(ctx.cursor) if ctx.cursor else datetime.datetime(2012, 1, 1)
    )
    now_time = datetime.datetime.utcnow()

    recently_closed_prs = []
    pr_ids_to_drop = []
    for pr_data in result:
        closed_time = datetime.datetime.strptime(pr_data["closed_at"], "%Y-%m-%dT%H:%M:%SZ")

        pr_id = str(pr_data["number"])

        closed_more_than_hour_ago = closed_time < now_time - datetime.timedelta(hours=1)
        not_considered_in_last_run = closed_time > last_cursor_time - datetime.timedelta(hours=1)
        has_snowflake_clone = pr_id in pr_ids_snowflake_clones

        if closed_more_than_hour_ago and not_considered_in_last_run:
            recently_closed_prs.append(pr_id)
            if has_snowflake_clone:
                pr_ids_to_drop.append(pr_id)

    for pr_id in pr_ids_to_drop:
        yield RunRequest(
            run_key=pr_id,
            run_config={"ops": {"drop_database_clone": {"config": {"pull_request_id": pr_id}}}},
        )

    ctx.update_cursor(now_time.isoformat())

    if not pr_ids_to_drop:
        return SkipReason(
            "No Snowflake tables to drop. Found cloned databases for PRs"
            f" {pr_ids_snowflake_clones} and recently closed PRs {recently_closed_prs}"
        )
