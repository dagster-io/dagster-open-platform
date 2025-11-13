import dagster as dg
from dagster.components import definitions
from dagster_dbt import get_asset_key_for_model
from dagster_open_platform.defs.dbt.assets import get_dbt_non_partitioned_models
from dagster_open_platform.defs.dbt.partitions import insights_partition

# Job that materializes assets from compass_analytics and staging_compass_analytics groups
# and everything downstream of those assets

compass_organizations_asset_key = get_asset_key_for_model(
    [get_dbt_non_partitioned_models()], "compass_organizations"
)
compass_analytics_job = dg.define_asset_job(
    name="compass_analytics_hourly_job",
    partitions_def=insights_partition,
    selection=(
        dg.AssetSelection.groups(
            "compass_analytics"
        )  # TODO: add staging_compass_analytics back in when migration is complete
        .downstream()
        .required_multi_asset_neighbors()
        .materializable()
        | (
            dg.AssetSelection.keys(compass_organizations_asset_key).upstream()
            & dg.AssetSelection.from_string('key:"*stg_segment_compass*"').downstream()
        )
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
    insights_partition_key = insights_partition.get_last_partition_key()
    yield dg.RunRequest(
        partition_key=str(insights_partition_key),
        run_key=str(insights_partition_key),
    )


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        jobs=[compass_analytics_job],
        schedules=[compass_analytics_hourly_schedule],
    )
