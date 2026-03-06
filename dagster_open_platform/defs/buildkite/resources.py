from typing import Any

import dagster as dg
import requests


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
        pipeline_slug: str,
        created_from: str | None = None,
        branch: str | None = None,
        per_page: int = 100,
        max_pages: int = 5,
    ) -> list[dict[str, Any]]:
        """Fetch builds for a pipeline with pagination.

        Args:
            pipeline_slug: The pipeline slug to fetch builds for.
            created_from: ISO 8601 timestamp to filter builds created after.
            branch: Filter builds by branch name (e.g. "master").
            per_page: Number of builds per page (max 100).
            max_pages: Maximum number of pages to fetch.

        Returns:
            List of build dicts (jobs included inline by Buildkite API).
        """
        all_builds: list[dict[str, Any]] = []
        params: dict[str, str] = {"per_page": str(per_page)}
        if created_from:
            params["created_from"] = created_from
        if branch:
            params["branch"] = branch

        for page in range(1, max_pages + 1):
            params["page"] = str(page)
            url = f"{self._base_url}/pipelines/{pipeline_slug}/builds"
            response = requests.get(url, headers=self._headers, params=params, timeout=30)
            response.raise_for_status()
            builds = response.json()
            if not builds:
                break
            all_builds.extend(builds)

        return all_builds

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
