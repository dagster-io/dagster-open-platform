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
    selection=(
        dg.AssetSelection.keys(
            "hightouch_sync_hubspot_company", "hightouch_sync_hubspot_organization"
        )
        .upstream()
        .required_multi_asset_neighbors()
        - dg.AssetSelection.groups("cloud_product_main").upstream()
        - dg.AssetSelection.groups("cloud_product_shard1").upstream()
        - dg.AssetSelection.groups("staging_aws").upstream()
        - dg.AssetSelection.groups("product").upstream().required_multi_asset_neighbors()
    ),
    tags={"team": "devrel", "dagster/max_retries": 1},
)


@dg.schedule(
    cron_schedule="0 * * * *",
    job=hightouch_hubspot_syncs_job,
)
def hightouch_hubspot_syncs_schedule(context):
    # Find runs of the same job that are currently running
    run_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name="my_job",
            statuses=[
                dg.DagsterRunStatus.QUEUED,
                dg.DagsterRunStatus.NOT_STARTED,
                dg.DagsterRunStatus.STARTING,
                dg.DagsterRunStatus.STARTED,
            ],
        )
    )
    # skip a schedule run if another run of the same job is already running
    if len(run_records) > 0:
        return dg.SkipReason(
            "Skipping this run because another run of the same job is already running"
        )
    most_recent_partition = insights_partition.get_last_partition_key()
    yield dg.RunRequest(
        partition_key=str(most_recent_partition), run_key=str(most_recent_partition)
    )
