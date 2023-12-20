from dagster import (
    AssetSelection,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
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

scheduled_jobs = [oss_telemetry_job, insights_job]

schedules = [oss_telemetry_schedule, insights_schedule]
