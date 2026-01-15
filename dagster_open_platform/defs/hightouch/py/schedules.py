import dagster as dg
from dagster.components import definitions

# This is a very specific selection for an hourly sync of Hightouch data.
# We are intentionally only including those assets that we feel are necessary
# for more real time insights. All other assets will be synced via the daily dbt run.
hightouch_syncs_job = dg.define_asset_job(
    name="hourly_hightouch_syncs_job",
    selection=dg.AssetSelection.assets(
        "hightouch_sync_salesforce_account",
        "hightouch_sync_salesforce_opportunity",
        "hightouch_sync_hubspot_company",
        "hightouch_sync_hubspot_organization",
    ).upstream()
    & (
        dg.AssetSelection.groups(
            "fivetran_salesforce", "fivetran_hubspot", "stripe_pipeline"
        ).downstream()
        | dg.AssetSelection.from_string(
            'key:"sling/*/*organizations" or key:"sling/*/users"'
        ).downstream()
    )
    - dg.AssetSelection.from_string('key:"*snapshot*"')
    - dg.AssetSelection.from_string('key:"*gong*"')
    - dg.AssetSelection.groups("product")
    - dg.AssetSelection.groups("business_activity_logs")
    - dg.AssetSelection.from_string('key:"*/*/dim_accounts_by_day"')
    - dg.AssetSelection.groups("mart_gtm")
    - dg.AssetSelection.from_string('key:"sling/compass/*"'),
    tags={"team": "devrel", "dagster/max_retries": 1},
)


@dg.schedule(
    cron_schedule="0 * * * *",
    job=hightouch_syncs_job,
)
def hourly_hightouch_syncs_schedule(context):
    # Find runs of the same job that are currently running
    run_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name="hourly_hightouch_syncs_job",
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
    return dg.Definitions(schedules=[hourly_hightouch_syncs_schedule])
