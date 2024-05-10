import logging

import dlt
from dlt.extract.incremental import Incremental
from dlt.sources.helpers import requests

logging.basicConfig(filename=__file__, level=logging.INFO)


@dlt.source
def pipelines(
    org_slug: str,
    pipeline_slugs: list[str],
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
        state: str = "finished",
        created_from: Incremental[str] = dlt.sources.incremental(
            "created_at", "2024-05-01T00:00:00Z"
        ),
    ):
        for pipeline_slug in pipeline_slugs:
            page = 1
            while True:
                logging.info("querying page: %s", page)
                params = {
                    "page": str(page),
                    "per_page": str(per_page),
                    "state": state,
                    "created_from": created_from.start_value,
                }
                response = requests.get(
                    f"https://api.buildkite.com/v2/organizations/{org_slug}/pipelines/{pipeline_slug}/builds",
                    headers=headers,
                    params=params,
                )
                response.raise_for_status()
                _json = response.json()
                if len(_json) == 0:
                    break
                yield _json
                page += 1

    return builds
