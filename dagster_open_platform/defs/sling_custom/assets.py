from dagster import AssetKey, Definitions
from dagster.components import definitions
from dagster_dbt import get_asset_key_for_model
from dagster_open_platform.defs.dbt.assets import get_dbt_non_partitioned_models

from .resources import SlingMode, build_sync_snowflake_to_postgres_asset

# Excludes surrogate_key column
USAGE_METRICS_SELECT_QUERY_TEMPLATE = """
SELECT
	ORGANIZATION_ID,
	AGENT_TYPE,
	JOB_NAME,
	REPOSITORY_NAME,
	JOB_DAY,
	MATERIALIZATIONS,
	STEP_DURATION_MINS,
	STEPS,
	RUN_DURATION_MINS,
	RUNS,
	STEPS_CREDITS,
	STANDARD_CREDITS,
	HIGH_VELOCITY_CREDITS
FROM
    {}
"""

USAGE_METRICS_SELECT_QUERY = USAGE_METRICS_SELECT_QUERY_TEMPLATE.format(
    "purina.product.usage_metrics_daily_jobs_aggregated"
)


@definitions
def defs():
    return Definitions(
        assets=[
            build_sync_snowflake_to_postgres_asset(
                key=AssetKey(["postgres", "usage_metrics_daily_jobs_aggregated_reporting"]),
                sling_resource_key="cloud_prod_reporting_sling",
                source_table=USAGE_METRICS_SELECT_QUERY,
                dest_table="public.usage_metrics_daily_jobs_aggregated",
                group_name="sling_egress",
                mode=SlingMode.TRUNCATE,
                deps=[
                    get_asset_key_for_model(
                        [get_dbt_non_partitioned_models()],
                        "usage_metrics_daily_jobs_aggregated",
                    )
                ],
                allow_alter_table=False,
            ),
        ]
    )
