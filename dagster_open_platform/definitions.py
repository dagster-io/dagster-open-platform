import warnings

import dagster_open_platform.aws.definitions as aws_definitions
import dagster_open_platform.dbt.definitions as dbt_definitions
import dagster_open_platform.dlt.definitions as dlt_definitions
import dagster_open_platform.fivetran.definitions as fivetran_definitions
import dagster_open_platform.hightouch.definitions as hightouch_definitions
import dagster_open_platform.pypi.definitions as pypi_definitions
import dagster_open_platform.scout.definitions as scout_definitions
import dagster_open_platform.segment.definitions as segment_definitions
import dagster_open_platform.slack.definitions as slack_definitions
import dagster_open_platform.sling.definitions as sling_definitions
import dagster_open_platform.stripe as stripe_definitions
from dagster import Definitions, ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from .assets import cloud_usage, dagster_quickstart, monitor_purina_clones
from .checks import salesforce_checks
from .resources import cloud_prod_sling_resource, snowflake_resource
from .schedules import schedules
from .utils.source_code import add_code_references_and_link_to_git

all_assets = [
    *cloud_usage.prod_sync_usage_metrics,
    monitor_purina_clones.inactive_snowflake_clones,
    dagster_quickstart.dagster_quickstart_validation,
]

all_checks = [
    salesforce_checks.account_has_valid_org_id,
    # *stripe_data_sync.stripe_pipeline_freshness_checks,
]

all_schedules = [
    *schedules,
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
    slack_definitions.defs,
    segment_definitions.defs,
    scout_definitions.defs,
    aws_definitions.defs,
    Definitions(
        assets=add_code_references_and_link_to_git(all_assets),
        asset_checks=all_checks,
        resources={
            "snowflake": snowflake_resource,
            "cloud_prod_sling": cloud_prod_sling_resource,
        },
        schedules=all_schedules,
        sensors=all_sensors,
    ),
)
