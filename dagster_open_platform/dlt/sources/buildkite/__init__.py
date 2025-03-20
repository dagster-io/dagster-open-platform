import dlt
from dlt.extract.incremental import Incremental
from dlt.sources.helpers import requests
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


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


# A fancy variation of the Buildkite source that does this:
# https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic
@dlt.source
def buildkite_source_v2(org_slug: str, buildkite_api_token=dlt.secrets.value):
    headers = {"Authorization": f"Bearer {buildkite_api_token}"}

    config: RESTAPIConfig = {
        "client": {
            "base_url": f"https://api.buildkite.com/v2/organizations/{org_slug}",
            "headers": headers,
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {},
        },
        "resources": [
            "pipelines",
            {
                "name": "builds",
                "endpoint": {
                    "path": "pipelines/{resources.pipelines.slug}/builds",
                    "params": {
                        "include_retried_jobs": "true",
                        "created_from": "2025-01-01T00:00:00Z",
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)
