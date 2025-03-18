import datetime
import json
import os
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Optional

import dagster as dg
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)
from dagster_open_platform.dbt.partitions import insights_partition
from dagster_open_platform.dbt.resources import dagster_open_platform_dbt_project

SNOWFLAKE_ACCOUNT_BASE, *_ = os.getenv("SNOWFLAKE_ACCOUNT", ".").split(".")
PURINA_DATABASE_NAME = (
    f"PURINA_CLONE_{os.environ['DAGSTER_CLOUD_PULL_REQUEST_ID']}"
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1"
    else "PURINA"
)
SNOWFLAKE_URL = f"https://app.snowflake.com/ax61354/{SNOWFLAKE_ACCOUNT_BASE}/#/data/databases/{PURINA_DATABASE_NAME}/schemas"

INCREMENTAL_SELECTOR = "config.materialized:incremental"
SNAPSHOT_SELECTOR = "resource_type:snapshot"


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        if dbt_resource_props["resource_type"] == "snapshot":
            return "snapshots"
        # Same logic that sets the custom schema in macros/get_custom_schema.sql
        asset_path = dbt_resource_props["fqn"][2:-1]
        if asset_path:
            return "_".join(asset_path)
        return "default"

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
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
            return dg.AssetKey(dbt_resource_props["meta"]["dagster"]["asset_key"])

        return dg.AssetKey([resource_database, resource_schema, resource_name])

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        url_metadata = {}
        if dbt_resource_props["resource_type"] == "model":
            url_metadata = {
                "url": dg.MetadataValue.url(
                    "/".join(
                        [
                            SNOWFLAKE_URL,
                            dbt_resource_props["schema"].upper(),
                            "table",
                            dbt_resource_props["name"].upper(),
                        ]
                    )
                )
            }

        return {
            **super().get_metadata(dbt_resource_props),
            **url_metadata,
        }


@dbt_assets(
    manifest=dagster_open_platform_dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    ),
    exclude=" ".join([INCREMENTAL_SELECTOR, SNAPSHOT_SELECTOR]),
    backfill_policy=dg.BackfillPolicy.single_run(),
    project=dagster_open_platform_dbt_project,
)
def dbt_non_partitioned_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from (
        dbt.cli(["build"], context=context)
        .stream()
        # .fetch_row_counts() # removing row counts for now due to performance issues
        .fetch_column_metadata()
        .with_insights()
    )


class DbtConfig(dg.Config):
    full_refresh: bool = False


@dbt_assets(
    manifest=dagster_open_platform_dbt_project.manifest_path,
    select=INCREMENTAL_SELECTOR,
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    ),
    partitions_def=insights_partition,
    backfill_policy=dg.BackfillPolicy.single_run(),
    project=dagster_open_platform_dbt_project,
)
def dbt_partitioned_models(
    context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig
):
    dbt_vars = {
        "min_date": (context.partition_time_window.start - timedelta(hours=3)).isoformat(),
        "max_date": context.partition_time_window.end.isoformat(),
    }

    args = (
        ["build", "--full-refresh"]
        if config.full_refresh
        else ["build", "--vars", json.dumps(dbt_vars)]
    )

    yield from (
        dbt.cli(args, context=context)
        .stream()
        # .fetch_row_counts() # removing row counts for now due to performance issues
        .fetch_column_metadata()
        .with_insights()
    )


@dbt_assets(
    manifest=dagster_open_platform_dbt_project.manifest_path,
    select=SNAPSHOT_SELECTOR,
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    ),
    backfill_policy=dg.BackfillPolicy.single_run(),
    project=dagster_open_platform_dbt_project,
)
def dbt_snapshot_models(context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig):
    yield from dbt.cli(["snapshot"], context=context).stream().with_insights()


snapshots_freshness_checks = dg.build_last_update_freshness_checks(
    assets=[dbt_snapshot_models],
    lower_bound_delta=datetime.timedelta(hours=36),
)
