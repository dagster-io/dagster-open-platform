import datetime
import json
from collections.abc import Mapping, Sequence
from datetime import timedelta
from functools import cache

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
from dagster_open_platform.lib.dbt.backfill import DBT_BACKFILL_RUN_TAG, DBT_BACKFILL_RUN_TAG_VALUE
from dagster_open_platform.lib.dbt.translator import CustomDagsterDbtTranslator

INCREMENTAL_SELECTOR = "config.materialized:incremental"
SNAPSHOT_SELECTOR = "resource_type:snapshot"
BACKFILL_SNOWFLAKE_WAREHOUSE = "BACKFILL_WH"
BACKFILL_STATEMENT_TIMEOUT_SECONDS = 24 * 60 * 60

logger = dg.get_dagster_logger()


class DbtConfig(dg.Config):
    full_refresh: bool = False
    backfill: bool = False


def _dbt_args(
    command: str,
    config: DbtConfig,
    dbt_vars: Mapping[str, object] | None = None,
    *,
    backfill: bool | None = None,
) -> list[str]:
    backfill = config.backfill if backfill is None else backfill
    vars_arg = {**(dbt_vars or {})}
    if backfill:
        vars_arg.update(
            {
                "backfill": True,
                "backfill_snowflake_warehouse": BACKFILL_SNOWFLAKE_WAREHOUSE,
                "backfill_statement_timeout_seconds": BACKFILL_STATEMENT_TIMEOUT_SECONDS,
            }
        )

    args = [command]
    if command == "build" and config.full_refresh:
        args.append("--full-refresh")
    if vars_arg:
        args.extend(["--vars", json.dumps(vars_arg)])

    return args


def _is_dbt_backfill_run(run_tags: Mapping[str, str], config: DbtConfig) -> bool:
    return config.backfill or run_tags.get(DBT_BACKFILL_RUN_TAG) == DBT_BACKFILL_RUN_TAG_VALUE


@cache
def get_dbt_non_partitioned_models(
    custom_translator: DagsterDbtTranslator | None = None,
    additional_selectors: Sequence[str] | None = None,
):
    dbt_project = dagster_open_platform_dbt_project()
    assert dbt_project

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=(
            custom_translator
            or CustomDagsterDbtTranslator(
                settings=DagsterDbtTranslatorSettings(enable_code_references=False)
            )
        ),
        select=",".join(additional_selectors or [DBT_DEFAULT_SELECT]),
        exclude=" ".join([INCREMENTAL_SELECTOR, SNAPSHOT_SELECTOR]),
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dbt_project,
    )
    def dbt_non_partitioned_models(
        context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig
    ):
        logger.info(f"dbt_project.project_dir: {dbt_project.project_dir}")
        yield from (
            dbt.cli(
                _dbt_args("build", config, backfill=_is_dbt_backfill_run(context.run.tags, config)),
                context=context,
            )
            .stream()
            # .fetch_row_counts() # removing row counts for now due to performance issues
            .fetch_column_metadata()
            .with_insights()
        )

    return dbt_non_partitioned_models


@cache
def get_dbt_partitioned_models(
    custom_translator: DagsterDbtTranslator | None = None,
    additional_selectors: Sequence[str] | None = None,
):
    dbt_project = dagster_open_platform_dbt_project()
    assert dbt_project

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        select=",".join([INCREMENTAL_SELECTOR, *(additional_selectors or [])]),
        dagster_dbt_translator=(
            custom_translator
            or CustomDagsterDbtTranslator(
                settings=DagsterDbtTranslatorSettings(enable_code_references=False)
            )
        ),
        partitions_def=insights_partition,
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dbt_project,
    )
    def dbt_partitioned_models(
        context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig
    ):
        logger.info(f"dbt_project.project_dir: {dbt_project.project_dir}")
        dbt_vars = {
            "min_date": (context.partition_time_window.start - timedelta(hours=3)).isoformat(),
            "max_date": (context.partition_time_window.end + timedelta(days=1)).isoformat(),
        }

        yield from (
            dbt.cli(
                _dbt_args(
                    "build",
                    config,
                    None if config.full_refresh else dbt_vars,
                    backfill=_is_dbt_backfill_run(context.run.tags, config),
                ),
                context=context,
            )
            .stream()
            # .fetch_row_counts() # removing row counts for now due to performance issues
            .fetch_column_metadata()
            .with_insights()
        )

    return dbt_partitioned_models


@cache
def get_dbt_snapshot_models(
    custom_translator: DagsterDbtTranslator | None = None,
    additional_selectors: Sequence[str] | None = None,
):
    @dbt_assets(
        manifest=dagster_open_platform_dbt_project().manifest_path,
        select=",".join([SNAPSHOT_SELECTOR, *(additional_selectors or [])]),
        dagster_dbt_translator=(
            custom_translator
            or CustomDagsterDbtTranslator(
                settings=DagsterDbtTranslatorSettings(enable_code_references=False)
            )
        ),
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dagster_open_platform_dbt_project(),
    )
    def dbt_snapshot_models(
        context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig
    ):
        yield from (
            dbt.cli(
                _dbt_args(
                    "snapshot", config, backfill=_is_dbt_backfill_run(context.run.tags, config)
                ),
                context=context,
            )
            .stream()
            .with_insights()
        )

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
