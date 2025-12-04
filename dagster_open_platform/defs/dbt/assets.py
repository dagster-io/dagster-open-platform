import datetime
import json
from collections.abc import Sequence
from datetime import timedelta
from functools import cache
from typing import Optional

import dagster as dg
from dagster.components import definitions
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)
from dagster_dbt.asset_utils import DBT_DEFAULT_SELECT
from dagster_open_platform.defs.dbt.partitions import insights_partition
from dagster_open_platform.defs.dbt.resources import dagster_open_platform_dbt_project
from dagster_open_platform.lib.dbt.translator import CustomDagsterDbtTranslator

INCREMENTAL_SELECTOR = "config.materialized:incremental"
SNAPSHOT_SELECTOR = "resource_type:snapshot"

logger = dg.get_dagster_logger()


@cache
def get_dbt_non_partitioned_models(
    custom_translator: Optional[DagsterDbtTranslator] = None,
    additional_selectors: Optional[Sequence[str]] = None,
):
    dbt_project = dagster_open_platform_dbt_project()
    assert dbt_project

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=(
            custom_translator
            or CustomDagsterDbtTranslator(
                settings=DagsterDbtTranslatorSettings(enable_code_references=True)
            )
        ),
        select=",".join(additional_selectors or [DBT_DEFAULT_SELECT]),
        exclude=" ".join([INCREMENTAL_SELECTOR, SNAPSHOT_SELECTOR]),
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dbt_project,
    )
    def dbt_non_partitioned_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        logger.info(f"dbt_project.project_dir: {dbt.project.project_dir}")
        yield from (
            dbt.cli(["build"], context=context)
            .stream()
            # .fetch_row_counts() # removing row counts for now due to performance issues
            .fetch_column_metadata()
            .with_insights()
        )

    return dbt_non_partitioned_models


class DbtConfig(dg.Config):
    full_refresh: bool = False


@cache
def get_dbt_partitioned_models(
    custom_translator: Optional[DagsterDbtTranslator] = None,
    additional_selectors: Optional[Sequence[str]] = None,
):
    dbt_project = dagster_open_platform_dbt_project()
    assert dbt_project

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        select=",".join([INCREMENTAL_SELECTOR, *(additional_selectors or [])]),
        dagster_dbt_translator=(
            custom_translator
            or CustomDagsterDbtTranslator(
                settings=DagsterDbtTranslatorSettings(enable_code_references=True)
            )
        ),
        partitions_def=insights_partition,
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dbt_project,
    )
    def dbt_partitioned_models(
        context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig
    ):
        logger.info(f"dbt_project.project_dir: {dbt.project.project_dir}")
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

    return dbt_partitioned_models


@cache
def get_dbt_snapshot_models(
    custom_translator: Optional[DagsterDbtTranslator] = None,
    additional_selectors: Optional[Sequence[str]] = None,
):
    @dbt_assets(
        manifest=dagster_open_platform_dbt_project().manifest_path,
        select=",".join([SNAPSHOT_SELECTOR, *(additional_selectors or [])]),
        dagster_dbt_translator=(
            custom_translator
            or CustomDagsterDbtTranslator(
                settings=DagsterDbtTranslatorSettings(enable_code_references=True)
            )
        ),
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dagster_open_platform_dbt_project(),
    )
    def dbt_snapshot_models(
        context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig
    ):
        yield from dbt.cli(["snapshot"], context=context).stream().with_insights()

    return dbt_snapshot_models


@definitions
def defs():
    dbt_snapshot_models = get_dbt_snapshot_models()
    dbt_partitioned_models = get_dbt_partitioned_models()
    dbt_non_partitioned_models = get_dbt_non_partitioned_models()

    return dg.Definitions(
        assets=[
            dbt_snapshot_models,
            dbt_partitioned_models,
            dbt_non_partitioned_models,
        ],
        asset_checks=dg.build_last_update_freshness_checks(
            assets=[dbt_snapshot_models],
            lower_bound_delta=datetime.timedelta(hours=36),
        ),
    )
