import dagster as dg
from dagster.components import definitions

# Job that materializes assets from compass_analytics and staging_compass_analytics groups
# and everything downstream of those assets
compass_analytics_job = dg.define_asset_job(
    name="compass_analytics_hourly_job",
    selection=(
        dg.AssetSelection.groups("compass_analytics", "staging_compass_analytics")
        .downstream()
        .required_multi_asset_neighbors()
        .materializable()
    ),
    tags={"team": "devrel"},
)


@dg.schedule(
    cron_schedule="0 * * * *",  # Every hour
    job=compass_analytics_job,
)
def compass_analytics_hourly_schedule(context):
    # Find runs of the same job that are currently running
    run_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name="compass_analytics_hourly_job",
            statuses=[
                dg.DagsterRunStatus.QUEUED,
                dg.DagsterRunStatus.NOT_STARTED,
                dg.DagsterRunStatus.STARTING,
                dg.DagsterRunStatus.STARTED,
            ],
        )
    )
    # Skip a schedule run if another run of the same job is already running
    if len(run_records) > 0:
        return dg.SkipReason(
            "Skipping this run because another run of the same job is already running"
        )
    yield dg.RunRequest()


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        jobs=[compass_analytics_job],
        schedules=[compass_analytics_hourly_schedule],
    )
