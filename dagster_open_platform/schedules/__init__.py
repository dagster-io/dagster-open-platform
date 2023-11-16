from dagster import AssetSelection, ScheduleDefinition, define_asset_job

oss_telemetry_job = define_asset_job(
    name="oss_telemetry_job",
    selection=AssetSelection.groups("oss_telemetry_staging").downstream(),
)

oss_telemetry_schedule = ScheduleDefinition(
    job=oss_telemetry_job,
    cron_schedule="0 5 * * *",  # every day at 5am
)

scheduled_jobs = [oss_telemetry_job]

schedules = [oss_telemetry_schedule]
