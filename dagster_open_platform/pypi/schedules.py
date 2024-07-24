from dagster import AssetSelection, ScheduleDefinition, define_asset_job
from dagster_open_platform.pypi.assets import dagster_pypi_downloads

oss_analytics_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="oss_analytics_job",
        selection=(AssetSelection.assets(dagster_pypi_downloads).downstream(include_self=False)),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 * * * *",
)
