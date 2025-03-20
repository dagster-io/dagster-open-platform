import dagster as dg
from dagster_open_platform.dbt.partitions import insights_partition
from dagster_open_platform.statsig.assets import org_performance_metrics, user_activity_metrics

statsig_upload_job = dg.define_asset_job(
    name="statsig_upload_job",
    selection=(dg.AssetSelection.assets(user_activity_metrics, org_performance_metrics)),
    partitions_def=insights_partition,
)


@dg.schedule(cron_schedule="0 1 * * *", job=statsig_upload_job)
def statsig_upload_schedule():
    most_recent_partition = statsig_upload_job.get_last_partition_key()
    yield dg.RunRequest(
        partition_key=str(most_recent_partition), run_key=str(most_recent_partition)
    )
