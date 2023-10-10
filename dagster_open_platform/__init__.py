import warnings

from dagster import Definitions, ExperimentalWarning, load_assets_from_modules

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from .assets import cloud_staging, health_check, oss_analytics, slack_analytics, stitch_ingest
from .resources import (
    bigquery_resource,
    dbt_resource,
    slack_resource,
    snowflake_resource,
    stitch_resource,
)

health_check_assets = load_assets_from_modules(
    [health_check],
    group_name="health_check",
)
oss_analytics_assets = load_assets_from_modules([oss_analytics], group_name="oss_analytics")
cloud_staging_assets = load_assets_from_modules([cloud_staging])
stitch_ingest_assets = load_assets_from_modules([stitch_ingest])

all_assets = [
    *cloud_staging_assets,
    *health_check_assets,
    *oss_analytics_assets,
    *stitch_ingest_assets,
    slack_analytics.slack_members,
]

defs = Definitions(
    assets=all_assets,
    resources={
        "bigquery": bigquery_resource,
        "dbt": dbt_resource,
        "slack": slack_resource,
        "snowflake": snowflake_resource,
        "stitch": stitch_resource,
    },
    schedules=[slack_analytics.slack_daily_schedule],
)
