from dagster import AssetKey, AssetSelection, ScheduleDefinition, define_asset_job

database_clone_cleanup_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="database_clone_cleanup_job",
        selection=AssetSelection.assets(AssetKey("inactive_snowflake_clones")),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)
