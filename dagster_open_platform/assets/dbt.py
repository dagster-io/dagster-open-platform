import json

from dagster import AssetExecutionContext, BackfillPolicy
from dagster_cloud.dagster_insights import dbt_with_snowflake_insights
from dagster_dbt import dbt_assets

from ..partitions import insights_partition
from ..resources import (
    DBT_MANIFEST_PATH,
    dbt_resource,
)
from ..utils.dbt_helpers import CustomDagsterDbtTranslator, DbtConfig

INSIGHTS_SELECTOR = "+tag:insights,config.materialized:incremental"


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    exclude=INSIGHTS_SELECTOR,
    backfill_policy=BackfillPolicy.single_run(),
)
def cloud_analytics_dbt_assets(context: AssetExecutionContext):
    yield from dbt_with_snowflake_insights(context, dbt_resource.cli(["build"], context=context))


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    select=INSIGHTS_SELECTOR,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    partitions_def=insights_partition,
    backfill_policy=BackfillPolicy.single_run(),
)
def dbt_insights_models(context: AssetExecutionContext, config: DbtConfig):
    time_window = context.asset_partitions_time_window_for_output(
        next(iter(context.selected_output_names))
    )
    # The `next(iter(context.selected_output_names))` is necessary because of dbt's sub-setting
    # A Dagster run can be `dbt_insights_models_2 -> cloud_analytics_dbt_assets_2 -> dbt_insights_models`
    # So the explicit step name is required for the execution to
    # know which `dbt_insights_model` Op is running

    dbt_vars = {"min_date": time_window.start.isoformat(), "max_date": time_window.end.isoformat()}
    args = (
        ["build", "--full-refresh"]
        if config.full_refresh
        else ["build", "--vars", json.dumps(dbt_vars)]
    )
    yield from dbt_with_snowflake_insights(context, dbt_resource.cli(args, context=context))
