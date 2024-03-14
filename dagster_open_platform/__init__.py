import warnings

from dagster import Definitions, ExperimentalWarning, load_assets_from_modules

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from .assets import (
    aws_cost_reporting,
    cloud_usage,
    dagster_quickstart,
    dbt,
    dlt,
    health_check,
    hightouch_syncs,
    oss_analytics,
    postgres_mirror,
    slack_analytics,
    sling_ingest,
    stitch_ingest,
    stripe_data_sync,
    support_bot,
)
from .checks import salesforce_checks
from .resources import (
    bigquery_resource,
    cloud_prod_read_replica_sling_resource,
    cloud_prod_reporting_sling_resource,
    cloud_prod_sling_resource,
    dbt_resource,
    embedded_elt_resource,
    github_resource,
    hightouch_resource,
    scoutos_resource,
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
support_bot_assets = load_assets_from_modules([support_bot])
dlt_assets = load_assets_from_modules([dlt])
stripe_sync_assets = load_assets_from_modules([stripe_data_sync])
sling_ingest_assets = load_assets_from_modules([sling_ingest])

all_assets = [
    aws_cost_reporting.aws_cost_report,
    *dbt_assets,
    *health_check_assets,
    *oss_analytics_assets,
    *stitch_ingest_assets,
    slack_analytics.member_metrics,
    *postgres_mirror_assets,
    *support_bot_assets,
    *dlt_assets,
    *cloud_usage.prod_sync_usage_metrics,
    hightouch_syncs.hightouch_org_activity_monthly,
    hightouch_syncs.hightouch_org_info,
    *stripe_sync_assets,
    *sling_ingest_assets,
    dagster_quickstart.dagster_quickstart_validation,
]

all_checks = [salesforce_checks.account_has_valid_org_id]

all_jobs = [*scheduled_jobs]

all_schedules = [*schedules, slack_analytics.slack_daily_schedule]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "bigquery": bigquery_resource,
        "dbt": dbt_resource,
        "hightouch": hightouch_resource,
        "slack": slack_resource,
        "snowflake": snowflake_resource,
        "stitch": stitch_resource,
        "cloud_prod_read_replica_sling": cloud_prod_read_replica_sling_resource,
        "cloud_prod_reporting_sling": cloud_prod_reporting_sling_resource,
        "github": github_resource,
        "scoutos": scoutos_resource,
        "cloud_prod_sling": cloud_prod_sling_resource,
        "embedded_elt": embedded_elt_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=[dagster_quickstart.dagster_quickstart_validation_sensor],
)
