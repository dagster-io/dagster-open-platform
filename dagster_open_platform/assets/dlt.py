"""Thinkific & Hubspot ingestion with `dlt`.

PREREQUISITES

    Ensure environment variables have been set:

        THINKIFIC_API_KEY
        THINKIFIC_SUBDOMAIN
        DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE
        DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD
        DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME
        DESTINATION__SNOWFLAKE__CREDENTIALS__HOST
        DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE
        DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE

"""

from dagster_open_platform.resources.dlt_resource import ConfigurableDltResource, build_dlt_assets

from .dlt_pipelines.thinkific import thinkific

dlt_thinkific_resource = ConfigurableDltResource(
    source=thinkific(),
    pipeline_name="thinkific",
    dataset_name="thinkific",
    destination="snowflake",
)

thinkific_assets = build_dlt_assets(
    dlt_thinkific_resource,
    name="thinkific",
    group_name="education",
)
