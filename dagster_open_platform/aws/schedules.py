from dagster import (
    AssetSelection,
    MultiPartitionsDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)
from dagster_open_platform.aws.assets import workspace_data_json
from dagster_open_platform.aws.partitions import daily_partition_def, org_partitions_def

workspace_replication_ingestion_job = define_asset_job(
    name="workspace_replication_ingestion_job",
    selection=AssetSelection.assets(workspace_data_json),
    partitions_def=MultiPartitionsDefinition(
        {"date": daily_partition_def, "org": org_partitions_def}
    ),
    tags={"team": "devrel"},
)

workspace_replication_schedule = build_schedule_from_partitioned_job(
    workspace_replication_ingestion_job
)
