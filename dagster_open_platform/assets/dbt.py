import json
import os
from typing import Any, Mapping, Optional

from dagster import AssetExecutionContext, AssetKey, BackfillPolicy, Config, MetadataValue
from dagster_cloud.dagster_insights import dbt_with_snowflake_insights
from dagster_dbt import DagsterDbtTranslator, dbt_assets

from ..partitions import insights_partition
from ..resources import (
    DBT_MANIFEST_PATH,
    dbt_resource,
)

SNOWFLAKE_ACCOUNT_BASE, *_ = os.getenv("SNOWFLAKE_ACCOUNT", ".").split(".")
PURINA_DATABASE_NAME = (
    f"PURINA_CLONE_{os.environ['DAGSTER_CLOUD_PULL_REQUEST_ID']}"
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1"
    else "PURINA"
)
SNOWFLAKE_URL = f"https://app.snowflake.com/ax61354/{SNOWFLAKE_ACCOUNT_BASE}/#/data/databases/{PURINA_DATABASE_NAME}/schemas"

INSIGHTS_SELECTOR = "+tag:insights,config.materialized:incremental"


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        # Same logic that sets the custom schema in macros/get_custom_schema.sql
        asset_path = dbt_resource_props["fqn"][1:-1]
        if asset_path:
            return "_".join(asset_path)
        return "default"

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_database = dbt_resource_props["database"]
        resource_schema = dbt_resource_props["schema"]
        resource_name = dbt_resource_props["name"]
        resource_type = dbt_resource_props["resource_type"]

        # if metadata has been provided in the yaml use that, otherwise construct key
        if (
            resource_type == "source"
            and "meta" in dbt_resource_props
            and "dagster" in dbt_resource_props["meta"]
            and "asset_key" in dbt_resource_props["meta"]["dagster"]
        ):
            return AssetKey(dbt_resource_props["meta"]["dagster"]["asset_key"])

        return AssetKey([resource_database, resource_schema, resource_name])

    def get_metadata(self, dbt_node_info: Mapping[str, Any]) -> Mapping[str, Any]:
        if dbt_node_info["resource_type"] != "model":
            return {}

        return {
            "url": MetadataValue.url(
                f"{SNOWFLAKE_URL}/{dbt_node_info['schema'].upper()}/table/{dbt_node_info['name'].upper()}"
            )
        }


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    exclude=INSIGHTS_SELECTOR,
    backfill_policy=BackfillPolicy.single_run(),
)
def cloud_analytics_dbt_assets(context: AssetExecutionContext):
    yield from dbt_with_snowflake_insights(context, dbt_resource.cli(["build"], context=context))


class DbtConfig(Config):
    full_refresh: bool = False


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    select=INSIGHTS_SELECTOR,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    partitions_def=insights_partition,
    backfill_policy=BackfillPolicy.single_run(),
)
def dbt_insights_models(context: AssetExecutionContext, config: DbtConfig):
    dbt_vars = {
        "min_date": context.partition_time_window.start.isoformat(),
        "max_date": context.partition_time_window.end.isoformat(),
    }
    args = ["build", "--vars", json.dumps(dbt_vars)]

    if config.full_refresh:
        args = ["build", "--full-refresh"]

    yield from dbt_with_snowflake_insights(context, dbt_resource.cli(args, context=context))
