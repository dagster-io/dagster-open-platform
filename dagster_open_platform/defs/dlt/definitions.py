"""Ingestion via `dlt`.

The `dlt` pipeline and source secrets can be extracted "auto-magically" from environment variables, as defined below:

    THINKIFIC_API_KEY
    THINKIFIC_SUBDOMAIN
    SOURCES__HUBSPOT__API_KEY
    DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE
    DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD
    DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME
    DESTINATION__SNOWFLAKE__CREDENTIALS__HOST
    DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE
    DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE

see: https://dlthub.com/docs/tutorial/grouping-resources#handle-secrets

"""

from dagster import Definitions, load_assets_from_modules
from dagster_dlt import DagsterDltResource
from dagster_open_platform.defs.dlt import assets
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git(load_assets_from_modules([assets])),
    resources={
        "dlt": DagsterDltResource(),
    },
)
