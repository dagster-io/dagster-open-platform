from datetime import datetime, timezone
from typing import Any

import dagster as dg
import requests

from dagster_open_platform.defs.buildkite.models import Build, Job


class BuildkiteResource(dg.ConfigurableResource):
    """Resource wrapping the Buildkite REST API for build analysis."""

    api_token: str
    org_slug: str = "dagster"

    @property
    def _base_url(self) -> str:
        return f"https://api.buildkite.com/v2/organizations/{self.org_slug}"

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_token}"}

    def get_builds(
        self,
        *,
        pipeline_slug: str | None = None,
        created_from: str | None = None,
        created_to: str | None = None,
        state: str | None = None,
        branch: str | None = None,
        include_retried_jobs: bool | None = True,
        per_page: int = 100,
        max_pages: int | None = None,
    ) -> list[Build]:
        """Fetch builds with pagination.

        When pipeline_slug is provided, fetches builds for that specific pipeline.
        When pipeline_slug is None, fetches builds across the entire organization.

        Args:
            pipeline_slug: Pipeline slug to filter by. None fetches org-wide.
            created_from: ISO 8601 timestamp to filter builds created after.
            created_to: ISO 8601 timestamp to filter builds created before.
            state: Filter by build state (e.g. "finished" for all terminal states).
            branch: Filter builds by branch name.
            per_page: Number of builds per page (max 100).
            max_pages: Maximum number of pages to fetch. None means no limit.

        Returns:
            List of Build objects
        """
        builds: list[Build] = []

        params: dict[str, str] = {"per_page": str(per_page)}
        if created_from:
            params["created_from"] = created_from
        if created_to:
            params["created_to"] = created_to
        if state:
            params["state"] = state
        if branch:
            params["branch"] = branch
        if include_retried_jobs:
            params["include_retried_jobs"] = "true"

        if pipeline_slug is not None:
            url = f"{self._base_url}/pipelines/{pipeline_slug}/builds"
        else:
            url = f"{self._base_url}/builds"

        page = 1
        while max_pages is None or page <= max_pages:
            params["page"] = str(page)
            extracted_at = datetime.now(tz=timezone.utc)
            response = requests.get(url, headers=self._headers, params=params, timeout=30)
            response.raise_for_status()
            page_data = response.json()
            if not page_data:
                break
            builds.extend(
                [BuildkiteResource.__api_to_build(b, extracted_at=extracted_at) for b in page_data]
            )
            page += 1

        return builds

    def get_job_log(self, pipeline_slug: str, build_number: int, job_id: str) -> str:
        """Fetch raw log text for a specific job.

        Args:
            pipeline_slug: The pipeline slug.
            build_number: The build number.
            job_id: The job UUID.

        Returns:
            Raw log text for the job.
        """
        url = f"{self._base_url}/pipelines/{pipeline_slug}/builds/{build_number}/jobs/{job_id}/log"
        response = requests.get(url, headers=self._headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("content", "")

    @staticmethod
    def __api_to_build(
        data: dict[str, Any],
        *,
        extracted_at: datetime,
    ) -> Build:
        build_id = data["id"]
        pipeline = data["pipeline"]

        return Build(
            id=build_id,
            extracted_at=extracted_at,
            pipeline__id=pipeline["id"],
            pipeline__slug=pipeline["slug"],
            pipeline__name=pipeline["name"],
            url=data["url"],
            web_url=data["web_url"],
            number=data["number"],
            state=data["state"],
            blocked=data["blocked"],
            cancel_reason=data.get("cancel_reason"),
            message=data["message"],
            commit=data["commit"],
            branch=data["branch"],
            source=data["source"],
            created_at=iso_to_dt(data.get("created_at")),
            scheduled_at=iso_to_dt(data.get("scheduled_at")),
            started_at=iso_to_dt(data.get("started_at")),
            finished_at=iso_to_dt(data.get("finished_at")),
            jobs=[
                BuildkiteResource.__api_to_job(
                    j,
                    build_id=build_id,
                    extracted_at=extracted_at,
                )
                for j in data["jobs"]
            ],
        )

    @staticmethod
    def __api_to_job(
        data: dict[str, Any],
        *,
        build_id: str,
        extracted_at: datetime,
    ) -> Job:
        return Job(
            id=data["id"],
            build_id=build_id,
            extracted_at=extracted_at,
            type=data["type"],
            name=data.get("name"),
            step_key=data.get("step_key"),
            state=data.get("state"),
            command=data.get("command"),
            soft_failed=data.get("soft_failed"),
            exit_status=data.get("exit_status"),
            retried=data.get("retried"),
            retries_count=data.get("retried_count"),
            created_at=iso_to_dt(data.get("created_at")),
            scheduled_at=iso_to_dt(data.get("scheduled_at")),
            runnable_at=iso_to_dt(data.get("runnable_at")),
            started_at=iso_to_dt(data.get("started_at")),
            finished_at=iso_to_dt(data.get("finished_at")),
            expired_at=iso_to_dt(data.get("expired_at")),
        )


def iso_to_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
