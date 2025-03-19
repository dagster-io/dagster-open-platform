from dagster import ScheduleDefinition, define_asset_job
from dagster_open_platform.snowflake.assets import inactive_snowflake_clones

database_clone_cleanup_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="database_clone_cleanup_job",
        selection=[inactive_snowflake_clones],
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)
