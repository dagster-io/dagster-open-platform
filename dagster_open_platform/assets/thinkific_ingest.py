"""Thinkific API ingestion with `dlt`.

EXAMPLE USAGE

    This script can be run both locally, and through a Dagster materialization. To run
    the script locally, perform the following:

        export $(xargs <.env)  # source environment variables in `.env`

        python dagster_open_platform/assets/thinkific_ingest.py

DEPENDENCIES

    You can either manage secrets via a `.dlt/secrets.toml` file, or through environment
    variables. If you opt to use environment variables, be sure to set the following:

        # Thinkific API
        THINKIFIC_API_KEY
        THINKIFIC_SUBDOMAIN

        # Snowflake Connection
        THINKIFIC__DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE
        THINKIFIC__DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD
        THINKIFIC__DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME
        THINKIFIC__DESTINATION__SNOWFLAKE__CREDENTIALS__HOST
        THINKIFIC__DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE
        THINKIFIC__DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE

    For more information regarding configurations and connections, see:

        https://dlthub.com/docs/general-usage/credentials/config_providers#the-provider-hierarchy
        https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake

"""

import dlt
from dagster import Config, MaterializeResult, asset
from dlt.sources.helpers import requests
from pydantic import Field

THINKIFIC_BASE_URL = "https://api.thinkific.com/api/public/v1/"


@dlt.source
def thinkific(
    thinkific_subdomain: str = dlt.config.value,
    thinkific_api_key: str = dlt.secrets.value,
):
    thinkific_headers = {
        "X-Auth-API-Key": thinkific_api_key,
        "X-Auth-Subdomain": thinkific_subdomain,
        "Content-Type": "application/json",
    }

    def _paginate(url, params={}):
        """Paginates requests to Thinkific API.

        Args:
            url: str: Thinkific API endpoint URL

        """
        params = params if params else {}
        params["page"] = 1
        params["limit"] = 250

        while params["page"] is not None:
            response = requests.get(
                url=url,
                params=params,
                headers=thinkific_headers,
            )
            response.raise_for_status()
            yield response.json().get("items")
            params["page"] = response.json().get("meta").get("pagination").get("next_page")

    @dlt.resource
    def courses():
        response = requests.get(
            url=THINKIFIC_BASE_URL + "courses",
            headers=thinkific_headers,
        )
        response.raise_for_status()
        yield response.json().get("items")

    @dlt.resource
    def course_reviews():
        # Enhancement - figure out how to use `course_id` values from `courses`
        # resource. It's not clear how to have downstream dependencies on resources, and
        # it's not clear if there's guarantee in order of resource runs.
        courses = {
            2364106: "Dagster Essentials",
            2662892: "Dagster + dbt",
        }
        for course_id in courses.keys():
            for page in _paginate(
                THINKIFIC_BASE_URL + "course_reviews", params={"course_id": course_id}
            ):
                yield page

    @dlt.resource
    def enrollments():
        # Enhancement - update to do incremental loads, see:
        # https://dlthub.com/docs/examples/incremental_loading/
        for page in _paginate(THINKIFIC_BASE_URL + "enrollments"):
            yield page

    @dlt.resource
    def users():
        for page in _paginate(THINKIFIC_BASE_URL + "users"):
            yield page

    return courses, course_reviews, enrollments, users


class ThinkificPipelineConfig(Config):
    pipeline_name: str = "thinkific"
    dataset_name: str = "thinkific"
    # Use `Literal["duckdb", "snowflake"] when support lands:
    # https://github.com/dagster-io/dagster/issues/8932
    destination: str = Field("snowflake", description="dlt destination (eg. 'snowflake', 'duckdb')")


@asset
def thinkific_pipeline(config: ThinkificPipelineConfig) -> MaterializeResult:
    """Asset wrapper around Thinkific `dlt` pipeline.

    This is a very minimal wrapper around the thinkific ingestion job. Future
    enhancements will include extraction of assets (dlt resources) from the pipeline,
    and proper source to target lineage.
    """
    pipeline = dlt.pipeline(
        pipeline_name=config.pipeline_name,
        destination=config.destination,
        dataset_name=config.dataset_name,
    )

    load_info = pipeline.run(thinkific())

    # Select a subset of load info metadata as not all items in the dictionary are
    # JSON-serializeable. Timestamps are also defined as 'pendulum.datetime.DateTime`,
    # so converting these to appropriate metadata values would be a good enhancement.
    metadata = {
        k: str(v)
        for k, v in load_info.asdict().items()
        if k
        in {
            "first_run",
            "started_at",
            "finished_at",
            "dataset_name",
            "destination_name",
            "destination_type",
        }
    }

    # Enhancement - add an asset check on the load info metadata
    return MaterializeResult(metadata=metadata)


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="thinkific", destination="duckdb", dataset_name="thinkific_data"
    )
    load_info = pipeline.run(thinkific())
    print(load_info)
