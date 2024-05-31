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
from dagster_open_platform.assets.oss_analytics import dagster_pypi_downloads

from ..assets import dbt, monitor_purina_clones, support_bot
from ..partitions import insights_partition
from ..resources import dagster_open_platform_dbt_project

support_bot_job = define_asset_job(
    name="support_bot_job",
    selection=AssetSelection.assets(support_bot.github_issues),
    tags={"team": "devrel"},
)


######################################################
##              ASK AI Support BOT                  ##
######################################################
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


######################################################
##              INSIGHTS                            ##
######################################################
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
    tags={"team": "insights", "dbt_pipeline": "insights"},
)


@schedule(cron_schedule="0 */3 * * *", job=insights_job)
def insights_schedule():
    most_recent_partition = insights_partition.get_last_partition_key()
    yield RunRequest(partition_key=str(most_recent_partition), run_key=str(most_recent_partition))


######################################################
##              Main DBT Pipeline                   ##
######################################################
dbt_analytics_core_job = define_asset_job(
    name="dbt_analytics_core_job",
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
        - AssetSelection.groups(
            "oss_analytics"
        )  # The source asset for this is on a weekly partition
    ),
    tags={"team": "devrel", "dbt_pipeline": "analytics_core"},
)


# Cloud usage metrics isn't partitioned, but it uses a partitioned asset
# that is managed by Insights. It doesn't matter which partition runs
# but does need to specify the most recent partition of Insights will be run
@schedule(cron_schedule="0 3 * * *", job=dbt_analytics_core_job)
def dbt_analytics_core_schedule():
    most_recent_partition = insights_partition.get_last_partition_key()
    yield RunRequest(partition_key=str(most_recent_partition), run_key=str(most_recent_partition))


dbt_analytics_snapshot_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="dbt_analytics_snapshot_job",
        selection=(AssetSelection.assets(dbt.dbt_snapshot_models)),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 * * * *",
)


######################################################
##              Sling Ingestion Pipelines           ##
######################################################
high_volume_assets = AssetSelection.keys(
    ["sling", "cloud_product", "event_logs"],
    ["sling", "cloud_product", "runs"],
    ["sling", "cloud_product", "run_tags"],
    ["sling", "cloud_product_shard1", "event_logs"],
    ["sling", "cloud_product_shard1", "runs"],
    ["sling", "cloud_product_shard1", "run_tags"],
    ["sling", "cloud_product", "asset_materializations"],
    ["sling", "cloud_product", "asset_observations"],
    ["sling", "cloud_product", "asset_partitions"],
    ["sling", "cloud_product", "alert_policies"],
    ["sling", "cloud_product_shard1", "asset_materializations"],
    ["sling", "cloud_product_shard1", "asset_observations"],
    ["sling", "cloud_product_shard1", "asset_partitions"],
)

cloud_product_sync_high_volume_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="cloud_product_sync_high_volume",
        selection=high_volume_assets,
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
        selection=AssetSelection.groups("cloud_product_main", "cloud_product_shard1")
        - high_volume_assets,
        tags={"team": "devrel"},
    ),
    cron_schedule="0 */2 * * *",
)
######################################################
##              Purina Cleanup                      ##
######################################################
purina_clone_cleanup_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="purina_clone_cleanup_job",
        selection=[monitor_purina_clones.inactive_snowflake_clones],
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)

######################################################
##              OSS Telemetry Ingest + dbt          ##
######################################################
oss_analytics_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="oss_analytics_job",
        selection=(AssetSelection.assets(dagster_pypi_downloads).downstream(include_self=False)),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 * * * *",
)

scheduled_jobs = [insights_job, support_bot_job]


schedules = [
    insights_schedule,
    dbt_analytics_core_schedule,
    cloud_product_sync_high_volume_schedule,
    cloud_product_sync_low_volume_schedule,
    purina_clone_cleanup_schedule,
    support_bot_schedule,
    oss_analytics_schedule,
    dbt_analytics_snapshot_schedule,
]
