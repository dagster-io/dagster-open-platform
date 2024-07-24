import warnings

import dagster_open_platform.dbt.definitions as dbt_definitions
import dagster_open_platform.dlt.definitions as dlt_definitions
import dagster_open_platform.fivetran.definitions as fivetran_definitions
import dagster_open_platform.hightouch.definitions as hightouch_definitions
import dagster_open_platform.pypi.definitions as pypi_definitions
import dagster_open_platform.sling.definitions as sling_definitions
import dagster_open_platform.stripe as stripe_definitions
from dagster import Definitions, ExperimentalWarning, load_assets_from_modules

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from .assets import (
    aws_cost_reporting,
    cloud_usage,
    dagster_quickstart,
    monitor_purina_clones,
    slack_analytics,
    source_segment,
    support_bot,
)
from .checks import salesforce_checks
from .resources import (
    cloud_prod_sling_resource,
    github_resource,
    scoutos_resource,
    slack_resource,
    snowflake_resource,
)
from .schedules import scheduled_jobs, schedules
from .utils.source_code import add_code_references_and_link_to_git

support_bot_assets = load_assets_from_modules([support_bot])
source_segment_assets = load_assets_from_modules([source_segment])

all_assets = [
    aws_cost_reporting.aws_cost_report,
    slack_analytics.member_metrics,
    *support_bot_assets,
    *cloud_usage.prod_sync_usage_metrics,
    monitor_purina_clones.inactive_snowflake_clones,
    dagster_quickstart.dagster_quickstart_validation,
    *source_segment_assets,
]

all_checks = [
    salesforce_checks.account_has_valid_org_id,
    # *stripe_data_sync.stripe_pipeline_freshness_checks,
]

all_jobs = [*scheduled_jobs]

all_schedules = [
    *schedules,
    slack_analytics.slack_daily_schedule,
]

all_sensors = [
    dagster_quickstart.dagster_quickstart_validation_sensor,
]

defs = Definitions.merge(
    dbt_definitions.defs,
    dlt_definitions.defs,
    fivetran_definitions.defs,
    sling_definitions.defs,
    hightouch_definitions.defs,
    stripe_definitions.defs,
    pypi_definitions.defs,
    Definitions(
        assets=add_code_references_and_link_to_git(all_assets),
        asset_checks=all_checks,
        resources={
            "slack": slack_resource,
            "snowflake": snowflake_resource,
            "github": github_resource,
            "scoutos": scoutos_resource,
            "cloud_prod_sling": cloud_prod_sling_resource,
        },
        jobs=all_jobs,
        schedules=all_schedules,
        sensors=all_sensors,
    ),
)
