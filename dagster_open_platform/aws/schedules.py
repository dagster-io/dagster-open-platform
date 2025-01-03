import dagster as dg
from dagster_open_platform.aws.assets import workspace_data_json
from dagster_open_platform.aws.partitions import org_daily_partition_def

aws_replication_schedule = dg.build_schedule_from_partitioned_job(
    name="aws_replication_schedule",
    hour_of_day=3,
    job=dg.define_asset_job(
        name="aws_replication_job",
        partitions_def=org_daily_partition_def,
        selection=[workspace_data_json],
        tags={"project": "aws-workspace-replication"},
    ),
)
