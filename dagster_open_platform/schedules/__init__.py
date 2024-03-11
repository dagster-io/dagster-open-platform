from dagster import (
    AssetSelection,
    Backoff,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
    schedule,
)
from dagster_dbt import build_dbt_asset_selection

from ..assets import dbt
from ..assets.postgres_mirror import REPO_LOCATION_DATA_NUM_CHUNKS, sync_repo_location_data
from ..partitions import insights_partition

oss_telemetry_job = define_asset_job(
    name="oss_telemetry_job",
    selection=AssetSelection.groups("oss_telemetry_staging").downstream(),
    tags={"team": "devrel"},
)

oss_telemetry_schedule = ScheduleDefinition(
    job=oss_telemetry_job,
    cron_schedule="0 5 * * *",  # every day at 5am
)

insights_selection = build_dbt_asset_selection(
    [dbt.cloud_analytics_dbt_assets], "tag:insights"
).upstream() - AssetSelection.assets(
    sync_repo_location_data
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
    AssetSelection.keys(["hightouch_usage_metrics_daily"]),
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


sync_repo_location_data_job = define_asset_job(
    name="sync_repo_location_data_job",
    selection=AssetSelection.assets(sync_repo_location_data),
    tags={
        "job": "sync_repo_location_data_job",
        "team": "insights",
    },
    op_retry_policy=RetryPolicy(max_retries=5, delay=60, backoff=Backoff.LINEAR),
)


@schedule(
    name="sync_repo_location_data_schedule",
    cron_schedule="0 0/2 * * *",
    job_name="sync_repo_location_data_job",
)
def sync_repo_location_data_schedule():
    for partition_id in range(REPO_LOCATION_DATA_NUM_CHUNKS):
        yield RunRequest(partition_key=str(partition_id), run_key=str(partition_id))


# Stitch syncs need to be run regularly and not as-needed because
# our data volume is too large for an individual sync
stitch_sync_frequent = ScheduleDefinition(
    job=define_asset_job(
        name="stitch_frequent_sync",
        selection=AssetSelection.key_prefixes(["stitch", "cloud_prod_public"]),
        tags={"team": "devrel"},
    ),
    cron_schedule="10,20,30,40,50 * * * *",
)
stitch_sync_infrequent = ScheduleDefinition(
    job=define_asset_job(
        name="stitch_infrequent_sync",
        selection=AssetSelection.key_prefixes(["stitch", "elementl_cloud_prod"]),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 11,23 * * *",
)

cloud_product_sync_high_volume_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="cloud_product_sync_high_volume",
        selection=AssetSelection.key_prefixes(["sling", "cloud_product_high_volume"]),
        tags={"team": "devrel"},
    ),
    cron_schedule="*/10 * * * *",
)

cloud_product_sync_low_volume_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="cloud_product_sync_low_volume",
        selection=AssetSelection.key_prefixes(["sling", "cloud_product_low_volume"]),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 */6 * * *",
)

education_selection = AssetSelection.groups("education")

education_job = define_asset_job(
    name="education_job",
    selection=education_selection,
    tags={"team": "devrel"},
)

education_schedule = ScheduleDefinition(job=education_job, cron_schedule="0 1 * * *")

scheduled_jobs = [
    oss_telemetry_job,
    insights_job,
    cloud_usage_metrics_job,
    sync_repo_location_data_job,
    education_job,
]

schedules = [
    oss_telemetry_schedule,
    insights_schedule,
    cloud_usage_metrics_schedule,
    sync_repo_location_data_schedule,
    stitch_sync_frequent,
    stitch_sync_infrequent,
    education_schedule,
    cloud_product_sync_high_volume_schedule,
    cloud_product_sync_low_volume_schedule,
]
