import warnings

from dagster import Definitions, ExperimentalWarning, load_assets_from_modules

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from .assets import (
    aws_cost_reporting,
    cloud_usage,
    dagster_quickstart,
    dbt,
    dlt,
    hightouch_syncs,
    ingest_fivetran,
    monitor_purina_clones,
    oss_analytics,
    slack_analytics,
    sling_egress,
    sling_ingest,
    source_segment,
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
)
from .schedules import scheduled_jobs, schedules

oss_analytics_assets = load_assets_from_modules([oss_analytics])
dbt_assets = load_assets_from_modules([dbt])
support_bot_assets = load_assets_from_modules([support_bot])
dlt_assets = load_assets_from_modules([dlt])
stripe_sync_assets = load_assets_from_modules([stripe_data_sync])
sling_ingest_assets = load_assets_from_modules([sling_ingest])
ingest_fivetran_assets = load_assets_from_modules([ingest_fivetran])
source_segment_assets = load_assets_from_modules([source_segment])
sling_egress_assets = load_assets_from_modules([sling_egress])

all_assets = [
    aws_cost_reporting.aws_cost_report,
    *dbt_assets,
    *oss_analytics_assets,
    slack_analytics.member_metrics,
    *support_bot_assets,
    *dlt_assets,
    *cloud_usage.prod_sync_usage_metrics,
    hightouch_syncs.hightouch_org_activity_monthly,
    hightouch_syncs.hightouch_org_info,
    hightouch_syncs.hightouch_null_contact_names,
    hightouch_syncs.hightouch_cloud_users,
    monitor_purina_clones.inactive_snowflake_clones,
    *stripe_sync_assets,
    *sling_ingest_assets,
    dagster_quickstart.dagster_quickstart_validation,
    *ingest_fivetran_assets,
    *source_segment_assets,
    *sling_egress_assets,
]

all_checks = [salesforce_checks.account_has_valid_org_id, *sling_ingest.event_logs_freshness_checks]

all_jobs = [*scheduled_jobs]

all_schedules = [
    *schedules,
    slack_analytics.slack_daily_schedule,
    stripe_data_sync.stripe_data_sync_schedule,
]

all_sensors = [
    sling_ingest.freshness_checks_sensor,
    dagster_quickstart.dagster_quickstart_validation_sensor,
]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "bigquery": bigquery_resource,
        "dbt": dbt_resource,
        "hightouch": hightouch_resource,
        "slack": slack_resource,
        "snowflake": snowflake_resource,
        "cloud_prod_read_replica_sling": cloud_prod_read_replica_sling_resource,
        "cloud_prod_reporting_sling": cloud_prod_reporting_sling_resource,
        "github": github_resource,
        "scoutos": scoutos_resource,
        "cloud_prod_sling": cloud_prod_sling_resource,
        "embedded_elt": embedded_elt_resource,
        "dlt": dlt.dlt_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
)
