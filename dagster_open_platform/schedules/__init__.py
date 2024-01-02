from dagster import (
    AssetSelection,
    RunRequest,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
    schedule,
)
from dagster_dbt import build_dbt_asset_selection

from ..assets import dbt
from ..partitions import insights_partition

oss_telemetry_job = define_asset_job(
    name="oss_telemetry_job",
    selection=AssetSelection.groups("oss_telemetry_staging").downstream(),
)

oss_telemetry_schedule = ScheduleDefinition(
    job=oss_telemetry_job,
    cron_schedule="0 5 * * *",  # every day at 5am
)

insights_selection = build_dbt_asset_selection(
    [dbt.cloud_analytics_dbt_assets], "tag:insights"
).upstream()  # select all insights models, and fetch upstream, including ingestion

insights_job = define_asset_job(
    name="insights_job", selection=insights_selection, partitions_def=insights_partition
)

insights_schedule = build_schedule_from_partitioned_job(job=insights_job)


assets_dependent_on_cloud_usage = [
    ["postgres", "usage_metrics_daily_jobs_aggregated"],
    ["hightouch_usage_metrics_daily"],
]

cloud_usage_metrics_selection = None

# This loops gets all upstream assets for the leaf node assets that depend
# on cloud analytics and unions them together to get a selection
for dependent_asset in assets_dependent_on_cloud_usage:
    curr_selection = (
        AssetSelection.keys(dependent_asset).upstream().required_multi_asset_neighbors()
    )
    if cloud_usage_metrics_selection is None:
        cloud_usage_metrics_selection = curr_selection
    else:
        cloud_usage_metrics_selection = cloud_usage_metrics_selection | curr_selection

cloud_usage_metrics_job = define_asset_job(
    name="cloud_usage_metrics_job", selection=cloud_usage_metrics_selection
)


# Cloud usage metrics isn't partitioned, but it uses a partitioned asset
# that is managed by Insights. It doesn't matter which partition runs
# but does need to specify the most recent partition of Insights will be run
@schedule(cron_schedule="0 3 * * *", job=cloud_usage_metrics_job)
def cloud_usage_metrics_schedule():
    most_recent_partition = insights_partition.get_last_partition_key()
    yield RunRequest(partition_key=str(most_recent_partition), run_key=str(most_recent_partition))


scheduled_jobs = [oss_telemetry_job, insights_job, cloud_usage_metrics_job]

schedules = [oss_telemetry_schedule, insights_schedule, cloud_usage_metrics_schedule]
