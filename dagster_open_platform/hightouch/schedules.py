import dagster as dg

hightouch_syncs_schedule = dg.ScheduleDefinition(
    name="hightouch_syncs_schedule",
    target=(
        dg.AssetSelection.groups("hightouch_syncs")
        - dg.AssetSelection.keys(
            "hightouch_sync_hubspot_company", "hightouch_sync_hubspot_organization"
        )
    ),
    tags={"team": "devrel"},
    cron_schedule="0 3 * * *",
)

hightouch_hubspot_syncs_schedule = dg.ScheduleDefinition(
    name="hightouch_hubspot_syncs_schedule",
    job=dg.define_asset_job(
        name="hightouch_hubspot_syncs_job",
        selection=(
            dg.AssetSelection.keys(
                "hightouch_sync_hubspot_company", "hightouch_sync_hubspot_organization"
            )
            .upstream()
            .required_multi_asset_neighbors()
            - dg.AssetSelection.groups("cloud_product_main").upstream()
            - dg.AssetSelection.groups("cloud_product_shard1").upstream()
            - dg.AssetSelection.groups("staging_aws").upstream()
        ),
    ),
    tags={"team": "devrel"},
    cron_schedule="0 * * * *",
)
