from dagster import AssetSelection, ScheduleDefinition, define_asset_job

hightouch_syncs_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="hightouch_syncs_job",
        selection=(AssetSelection.groups("hightouch_syncs")),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)
