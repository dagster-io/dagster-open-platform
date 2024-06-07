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
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_open_platform.dlt import assets

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "dlt": DagsterDltResource(),
    },
)
