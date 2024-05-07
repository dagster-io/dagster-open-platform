import logging

import dlt
from dlt.extract.incremental import Incremental
from dlt.sources.helpers import requests

logging.basicConfig(level=logging.INFO)


@dlt.source
def pipelines(
    org_slug: str,
    pipeline_slug: str,
    buildkite_api_token: str = dlt.secrets.value,
):
    """BuildKite pipelines.

    https://buildkite.com/docs/apis/rest-api/pipelines
    https://dlthub.com/docs/general-usage/incremental-loading#incremental-loading-with-a-cursor-field
    """
    headers = {"Authorization": f"Bearer {buildkite_api_token}"}

    @dlt.resource(primary_key="id", write_disposition="merge")
    def builds(
        per_page: int = 100,
        branch: str = "master",
        state: str = "finished",
        created_from: Incremental[str] = dlt.sources.incremental(
            "created_at", "2024-05-01T00:00:00Z"
        ),
    ):
        page = 1
        while True:
            logging.info("querying page: %s", page)
            response = requests.get(
                f"https://api.buildkite.com/v2/organizations/{org_slug}/pipelines/{pipeline_slug}/builds",
                headers=headers,
                params={
                    "page": str(page),
                    "per_page": str(per_page),
                    "branch": branch,
                    "state": state,
                    "created_from": created_from.start_value,
                },
            )
            response.raise_for_status()
            _json = response.json()
            if len(_json) == 0:
                break
            yield _json
            page += 1

    return builds
