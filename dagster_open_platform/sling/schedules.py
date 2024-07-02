from dagster import (
    AssetSelection,
    DagsterRunStatus,
    RunRequest,
    RunsFilter,
    ScheduleDefinition,
    define_asset_job,
    schedule,
)

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

cloud_product_sync_high_volume_job = define_asset_job(
    name="cloud_product_sync_high_volume",
    selection=high_volume_assets,
    tags={"team": "devrel-high-volume", "dagster/max_runtime": 60 * 15},
)

in_progress_statuses = [
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.QUEUED,
]


@schedule(job=cloud_product_sync_high_volume_job, cron_schedule="*/5 * * * *")
def cloud_product_sync_high_volume_schedule(context):
    in_progress_jobs = context.instance.get_run_records(
        RunsFilter(job_name="cloud_product_sync_high_volume", statuses=in_progress_statuses)
    )
    return RunRequest() if not in_progress_jobs else None


cloud_product_sync_low_volume_job = define_asset_job(
    name="cloud_product_sync_low_volume",
    selection=AssetSelection.groups("cloud_product_main", "cloud_product_shard1")
    - high_volume_assets,
    tags={"team": "devrel", "dagster/max_runtime": 60 * 15},
)


@schedule(job=cloud_product_sync_low_volume_job, cron_schedule="0 */2 * * *")
def cloud_product_sync_low_volume_schedule(context):
    in_progress_jobs = context.instance.get_run_records(
        RunsFilter(job_name="cloud_product_sync_low_volume", statuses=in_progress_statuses)
    )
    return RunRequest() if not in_progress_jobs else None


sling_egress_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="sling_egress_job",
        selection=AssetSelection.groups("sling_egress"),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)
