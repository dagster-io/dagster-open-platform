from dagster import (
    AssetSelection,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)
from dagster_open_platform.aws.partitions import daily_partition_def
from dagster_open_platform.snowflake.assets import (
    inactive_snowflake_clones,
    workspace_replication_aws_external_tables,
    workspace_replication_aws_stages,
)

purina_clone_cleanup_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="purina_clone_cleanup_job",
        selection=[inactive_snowflake_clones],
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)

workspace_replication_snowflake_staging_schedule = build_schedule_from_partitioned_job(
    define_asset_job(
        name="workspace_replication_snowflake_staging_job",
        selection=AssetSelection.assets(
            workspace_replication_aws_stages, workspace_replication_aws_external_tables
        ),
        partitions_def=daily_partition_def,
        tags={"team": "devrel"},
    )
)
