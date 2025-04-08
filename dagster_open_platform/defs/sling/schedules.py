from dagster import AssetSelection, ScheduleDefinition, define_asset_job

sling_egress_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="sling_egress_job",
        selection=AssetSelection.groups("sling_egress"),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)
