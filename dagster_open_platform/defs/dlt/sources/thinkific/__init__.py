"""Thinkific API ingestion with `dlt`.

EXAMPLE USAGE

    This script can be run both locally, and through a Dagster materialization. To run
    the script locally, perform the following:

        export $(xargs <.env)  # source environment variables in `.env`

        python dagster_open_platform/assets/dlt/thinkific.py

DEPENDENCIES

    You can either manage secrets via a `.dlt/secrets.toml` file, or through environment
    variables. If you opt to use environment variables, be sure to set the following:

        # Thinkific API
        THINKIFIC_API_KEY
        THINKIFIC_SUBDOMAIN

        # Snowflake Connection
        DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE
        DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD
        DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME
        DESTINATION__SNOWFLAKE__CREDENTIALS__HOST
        DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE
        DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE

    For more information regarding configurations and connections, see:

        https://dlthub.com/docs/general-usage/credentials/config_providers#the-provider-hierarchy
        https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake

"""

import os

import dlt
from dlt.sources.helpers import requests

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

    @dlt.resource(primary_key="id", write_disposition="merge")
    def courses():
        response = requests.get(
            url=THINKIFIC_BASE_URL + "courses",
            headers=thinkific_headers,
        )
        response.raise_for_status()
        yield response.json().get("items")

    @dlt.transformer(primary_key="id", write_disposition="merge", data_from=courses)
    def course_reviews(courses):
        for course in courses:
            yield from _paginate(
                THINKIFIC_BASE_URL + "course_reviews", params={"course_id": course["id"]}
            )

    @dlt.resource(primary_key="id", write_disposition="merge")
    def enrollments():
        # Enhancement - update to do incremental loads, see:
        # https://dlthub.com/docs/examples/incremental_loading/
        yield from _paginate(THINKIFIC_BASE_URL + "enrollments")

    @dlt.resource(primary_key="id", write_disposition="merge")
    def users():
        yield from _paginate(THINKIFIC_BASE_URL + "users")

    return courses, course_reviews, enrollments, users


if __name__ == "__main__":
    if os.getenv("ENVIRONMENT") == "local":
        pipeline = dlt.pipeline(
            pipeline_name="thinkific", destination="duckdb", dataset_name="data"
        )
    else:
        pipeline = dlt.pipeline(
            pipeline_name="thinkific", destination="snowflake", dataset_name="thinkific"
        )
    load_info = pipeline.run(thinkific())
    print(load_info)
