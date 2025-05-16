import dagster as dg
from dagster.components import definitions
from dagster_open_platform.defs.dbt.partitions import insights_partition
from dagster_open_platform.defs.statsig.assets import (
    get_org_performance_metrics,
    get_user_activity_metrics,
)


@definitions
def defs():
    statsig_upload_job = dg.define_asset_job(
        name="statsig_upload_job",
        selection=(
            dg.AssetSelection.assets(get_user_activity_metrics(), get_org_performance_metrics())
        ),
        partitions_def=insights_partition,
    )

    @dg.schedule(cron_schedule="0 1 * * *", job=statsig_upload_job)
    def statsig_upload_schedule():
        most_recent_partition = insights_partition.get_last_partition_key()
        yield dg.RunRequest(
            partition_key=str(most_recent_partition), run_key=str(most_recent_partition)
        )

    return dg.Definitions(
        schedules=[
            statsig_upload_schedule,
        ]
    )
