from datetime import timedelta

from dagster import (
    AssetSelection,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from dagster_dbt import DbtManifestAssetSelection

from ..assets import dbt, monitor_purina_clones, support_bot
from ..partitions import insights_partition
from ..resources import dagster_open_platform_dbt_project

support_bot_job = define_asset_job(
    name="support_bot_job",
    selection=AssetSelection.assets(support_bot.github_issues),
    tags={"team": "devrel"},
)


@schedule(job=support_bot_job, cron_schedule="@daily")
def support_bot_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    return RunRequest(
        tags={
            ASSET_PARTITION_RANGE_START_TAG: (
                context.scheduled_execution_time - timedelta(days=30)
            ).strftime("%Y-%m-%d"),
            ASSET_PARTITION_RANGE_END_TAG: context.scheduled_execution_time.strftime("%Y-%m-%d"),
        }
    )


oss_telemetry_job = define_asset_job(
    name="oss_telemetry_job",
    selection=AssetSelection.groups("staging_telemetry").downstream(),
    tags={"team": "devrel"},
)

oss_telemetry_schedule = ScheduleDefinition(
    job=oss_telemetry_job,
    cron_schedule="0 5 * * *",  # every day at 5am
)


insights_job = define_asset_job(
    name="insights_job",
    selection=(
        # select all insights models, and fetch upstream, including ingestion
        DbtManifestAssetSelection.build(
            manifest=dagster_open_platform_dbt_project.manifest_path,
            dagster_dbt_translator=dbt.CustomDagsterDbtTranslator(),
            select="tag:insights",
        )
        .upstream()
        .required_multi_asset_neighbors()
        - AssetSelection.groups("cloud_product_main")
        - AssetSelection.groups("cloud_product_shard1")
    ),
    partitions_def=insights_partition,
    tags={"team": "insights"},
)


@schedule(cron_schedule="0 */3 * * *", job=insights_job)
def insights_schedule():
    most_recent_partition = insights_partition.get_last_partition_key()
    yield RunRequest(partition_key=str(most_recent_partition), run_key=str(most_recent_partition))


cloud_usage_metrics_job = define_asset_job(
    name="cloud_usage_metrics_job",
    selection=(
        DbtManifestAssetSelection.build(
            manifest=dagster_open_platform_dbt_project.manifest_path,
            dagster_dbt_translator=dbt.CustomDagsterDbtTranslator(),
        )
        .upstream()
        .downstream()
        .required_multi_asset_neighbors()
        - AssetSelection.groups("cloud_reporting")
        - AssetSelection.key_prefixes(["purina", "postgres_mirror"])
        - AssetSelection.groups("cloud_product_main")
        - AssetSelection.groups("cloud_product_shard1")
    ),
    tags={"team": "devrel"},
)


# Cloud usage metrics isn't partitioned, but it uses a partitioned asset
# that is managed by Insights. It doesn't matter which partition runs
# but does need to specify the most recent partition of Insights will be run
@schedule(cron_schedule="0 3 * * *", job=cloud_usage_metrics_job)
def cloud_usage_metrics_schedule():
    most_recent_partition = insights_partition.get_last_partition_key()
    yield RunRequest(partition_key=str(most_recent_partition), run_key=str(most_recent_partition))


cloud_product_sync_high_volume_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="cloud_product_sync_high_volume",
        selection=AssetSelection.groups("cloud_product_high_volume_ingest"),
        tags={"team": "devrel"},
        op_retry_policy=RetryPolicy(
            max_retries=3,
            delay=20,
        ),
    ),
    cron_schedule="*/5 * * * *",
)

cloud_product_sync_low_volume_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="cloud_product_sync_low_volume",
        selection=AssetSelection.groups("cloud_product_low_volume_ingest"),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 */2 * * *",
)

purina_clone_cleanup_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="purina_clone_cleanup_job",
        selection=[monitor_purina_clones.inactive_snowflake_clones],
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)

scheduled_jobs = [oss_telemetry_job, insights_job, support_bot_job]

schedules = [
    oss_telemetry_schedule,
    insights_schedule,
    cloud_usage_metrics_schedule,
    cloud_product_sync_high_volume_schedule,
    cloud_product_sync_low_volume_schedule,
    purina_clone_cleanup_schedule,
    support_bot_schedule,
]
