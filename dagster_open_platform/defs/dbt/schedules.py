from datetime import timedelta

import dagster as dg
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from dagster.components import definitions
from dagster_dbt import DbtManifestAssetSelection, get_asset_key_for_model
from dagster_open_platform.defs.dbt.assets import (
    CustomDagsterDbtTranslator,
    get_dbt_non_partitioned_models,
    get_dbt_snapshot_models,
)
from dagster_open_platform.defs.dbt.partitions import insights_partition
from dagster_open_platform.defs.dbt.resources import dagster_open_platform_dbt_project

######################################################
##              Main DBT Pipeline                   ##
######################################################


@definitions
def defs():
    dbt_analytics_core_job = dg.define_asset_job(
        name="dbt_analytics_core_job",
        selection=(
            DbtManifestAssetSelection.build(
                manifest=dagster_open_platform_dbt_project().manifest_path,
                dagster_dbt_translator=CustomDagsterDbtTranslator(),
            ).required_multi_asset_neighbors()
            # snapshot models
            - dg.AssetSelection.assets(get_dbt_snapshot_models())
        ),
        tags={
            "team": "devrel",
            "dbt_pipeline": "analytics_core",
            "dagster/max_retries": 1,
        },
    )

    @dg.schedule(cron_schedule="0 3 * * *", job=dbt_analytics_core_job)
    def dbt_analytics_core_schedule():
        most_recent_partition = insights_partition.get_last_partition_key()
        yield dg.RunRequest(
            partition_key=str(most_recent_partition), run_key=str(most_recent_partition)
        )

    dbt_analytics_snapshot_job = dg.define_asset_job(
        name="dbt_analytics_snapshot_job",
        selection=dg.AssetSelection.assets(get_dbt_snapshot_models()).downstream()
        - dg.AssetSelection.kind("omni", include_sources=True),
    )

    @dg.schedule(cron_schedule="0 7 * * *", job=dbt_analytics_snapshot_job)
    def dbt_analytics_snapshot_schedule():
        most_recent_partition = insights_partition.get_last_partition_key()
        yield dg.RunRequest(
            partition_key=str(most_recent_partition), run_key=str(most_recent_partition)
        )

    ######################################################
    ##     Bi-Monthly Backfill: Usage & Org Metrics     ##
    ######################################################

    # Job to backfill usage_metrics_daily and organizations_by_day with their dependencies
    usage_metrics_key = get_asset_key_for_model(
        [get_dbt_non_partitioned_models()], "usage_metrics_daily"
    )
    organizations_by_day_key = get_asset_key_for_model(
        [get_dbt_non_partitioned_models()], "organizations_by_day"
    )
    event_log_key = get_asset_key_for_model(
        [get_dbt_non_partitioned_models()], "stg_cloud_product__event_logs"
    )

    bimonthly_backfill_job = dg.define_asset_job(
        name="bimonthly_usage_org_backfill_job",
        selection=(
            dg.AssetSelection.keys(event_log_key).downstream()
            & dg.AssetSelection.keys(usage_metrics_key, organizations_by_day_key).upstream()
            & dg.AssetSelection.kind("dbt")
        ),
        partitions_def=insights_partition,
    )

    @dg.schedule(cron_schedule="0 2 1,15 * *", job=bimonthly_backfill_job)
    def bimonthly_usage_org_backfill_schedule(context: dg.ScheduleEvaluationContext):
        """Backfills usage_metrics_daily and organizations_by_day (with dependencies) for the last 31 days on the 1st and 15th of each month at 2am."""
        end_date = context.scheduled_execution_time
        start_date = end_date - timedelta(days=31)

        yield dg.RunRequest(
            tags={
                ASSET_PARTITION_RANGE_START_TAG: start_date.strftime("%Y-%m-%d-%H:%M"),
                ASSET_PARTITION_RANGE_END_TAG: end_date.strftime("%Y-%m-%d-%H:%M"),
            }
        )

    return dg.Definitions(
        schedules=[
            dbt_analytics_core_schedule,
            dbt_analytics_snapshot_schedule,
            bimonthly_usage_org_backfill_schedule,
        ]
    )
