import dagster as dg
from dagster.components import definitions

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
            job_name="hightouch_hubspot_syncs_job",
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
    yield dg.RunRequest()


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(schedules=[hightouch_hubspot_syncs_schedule])
