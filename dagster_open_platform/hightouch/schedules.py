from dagster import AssetSelection, ScheduleDefinition

hightouch_syncs_schedule = ScheduleDefinition(
    name="hightouch_syncs_schedule",
    target=(AssetSelection.groups("hightouch_syncs")),
    tags={"team": "devrel"},
    cron_schedule="0 3 * * *",
)
