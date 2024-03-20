from dagster import (
    AssetKey,
    AssetSelection,
    RunRequest,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
    schedule,
)
from dagster_dbt import build_dbt_asset_selection

from ..assets import dbt, monitor_purina_clones
from ..partitions import insights_partition
from ..utils.environment_helpers import get_environment, get_schema_for_environment

oss_telemetry_job = define_asset_job(
    name="oss_telemetry_job",
    selection=AssetSelection.keys(
        AssetKey(
            [
                "sandbox" if get_environment() == "LOCAL" else "purina",
                get_schema_for_environment("staging").lower(),
                "stg_telemetry__events",
            ]
        )
    ).downstream(),
    tags={"team": "devrel"},
)

oss_telemetry_schedule = ScheduleDefinition(
    job=oss_telemetry_job,
    cron_schedule="0 5 * * *",  # every day at 5am
)

insights_selection = (
    build_dbt_asset_selection([dbt.cloud_analytics_dbt_assets], "tag:insights")
    .upstream()
    .required_multi_asset_neighbors()
    - AssetSelection.groups("cloud_product_high_volume_ingest")
    - AssetSelection.groups("cloud_product_low_volume_ingest")
)  # select all insights models, and fetch upstream, including ingestion

insights_job = define_asset_job(
    name="insights_job",
    selection=insights_selection,
    partitions_def=insights_partition,
    tags={"team": "insights"},
)

insights_schedule = build_schedule_from_partitioned_job(job=insights_job)


assets_dependent_on_cloud_usage = [
    AssetSelection.keys(["postgres", "usage_metrics_daily_jobs_aggregated"]),
    build_dbt_asset_selection(
        [dbt.cloud_analytics_dbt_assets], "org_asset_materializations_by_month"
    ),
    build_dbt_asset_selection([dbt.cloud_analytics_dbt_assets], "attributed_conversions"),
]

cloud_usage_metrics_selection = (
    build_dbt_asset_selection([dbt.cloud_analytics_dbt_assets], "fqn:*")
    .upstream()
    .downstream()
    .required_multi_asset_neighbors()
    - AssetSelection.groups("cloud_reporting")
    - AssetSelection.key_prefixes(["purina", "postgres_mirror"])
    - AssetSelection.groups("cloud_product_high_volume_ingest")
    - AssetSelection.groups("cloud_product_low_volume_ingest")
)

cloud_usage_metrics_job = define_asset_job(
    name="cloud_usage_metrics_job", selection=cloud_usage_metrics_selection, tags={"team": "devrel"}
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

scheduled_jobs = [
    oss_telemetry_job,
    insights_job,
]

schedules = [
    oss_telemetry_schedule,
    insights_schedule,
    cloud_usage_metrics_schedule,
    cloud_product_sync_high_volume_schedule,
    cloud_product_sync_low_volume_schedule,
    purina_clone_cleanup_schedule,
]
