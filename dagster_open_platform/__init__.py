import warnings

from dagster import Definitions, ExperimentalWarning, load_assets_from_modules

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from .assets import health_check, oss_analytics, slack_analytics
from .resources import bigquery_resource, slack_resource, snowflake_resource
from .resources.stitch_resource import stitch_resource

health_check_assets = load_assets_from_modules(
    [health_check],
    group_name="health_check",
)

oss_analytics_assets = load_assets_from_modules([oss_analytics], group_name="oss_analytics")

all_assets = [
    *health_check_assets,
    *oss_analytics_assets,
    slack_analytics.slack_members,
]

defs = Definitions(
    assets=all_assets,
    resources={
        "stitch": stitch_resource,
        "bigquery": bigquery_resource,
        "snowflake": snowflake_resource,
        "slack": slack_resource,
    },
    schedules=[slack_analytics.slack_daily_schedule],
)
