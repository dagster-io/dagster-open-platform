import dagster as dg

from ..dbt.partitions import insights_partition

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

hightouch_hubspot_syncs_job = dg.define_asset_job(
    name="hightouch_hubspot_syncs_job",
    partitions_def=insights_partition,
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
)


@dg.schedule(
    cron_schedule="0 * * * *",
    job=hightouch_hubspot_syncs_job,
    tags={"team": "devrel", "dagster/max_retries": 1},
)
def hightouch_hubspot_syncs_schedule():
    most_recent_partition = insights_partition.get_last_partition_key()
    yield dg.RunRequest(
        partition_key=str(most_recent_partition), run_key=str(most_recent_partition)
    )
