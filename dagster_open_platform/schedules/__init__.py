from dagster import ScheduleDefinition, define_asset_job
from dagster_open_platform.assets import monitor_purina_clones

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

schedules = [
    purina_clone_cleanup_schedule,
]
