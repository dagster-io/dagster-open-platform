import warnings

from dagster import Definitions, ExperimentalWarning, load_assets_from_modules

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from .assets import (
    dbt,
    health_check,
    oss_analytics,
    postgres_mirror,
    slack_analytics,
    stitch_ingest,
)
from .resources import (
    bigquery_resource,
    cloud_prod_read_replica_sling_resource,
    cloud_prod_reporting_sling_resource,
    dbt_resource,
    slack_resource,
    snowflake_resource,
    stitch_resource,
)
from .schedules import scheduled_jobs, schedules

health_check_assets = load_assets_from_modules(
    [health_check],
    group_name="health_check",
)
oss_analytics_assets = load_assets_from_modules([oss_analytics])
dbt_assets = load_assets_from_modules([dbt])
stitch_ingest_assets = load_assets_from_modules([stitch_ingest])
postgres_mirror_assets = load_assets_from_modules([postgres_mirror])

all_assets = [
    *dbt_assets,
    *health_check_assets,
    *oss_analytics_assets,
    *stitch_ingest_assets,
    slack_analytics.slack_members,
    *postgres_mirror_assets,
]

all_jobs = [*scheduled_jobs]

all_schedules = [*schedules, slack_analytics.slack_daily_schedule]

defs = Definitions(
    assets=all_assets,
    resources={
        "bigquery": bigquery_resource,
        "dbt": dbt_resource,
        "slack": slack_resource,
        "snowflake": snowflake_resource,
        "stitch": stitch_resource,
        "cloud_prod_read_replica_sling": cloud_prod_read_replica_sling_resource,
        "cloud_prod_reporting_sling": cloud_prod_reporting_sling_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
)
