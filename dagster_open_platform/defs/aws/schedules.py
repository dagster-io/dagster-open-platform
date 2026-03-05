from datetime import timedelta

import dagster as dg
from dagster import RunConfig
from dagster.components import definitions
from dagster_open_platform.defs.aws.assets import WorkspaceReplicationConfig, workspace_data_json
from dagster_open_platform.defs.aws.partitions import (
    BATCH_SIZE,
    daily_partition_def,
    org_partitions_def,
)

aws_replication_job = dg.define_asset_job(
    name="aws_replication_job",
    partitions_def=daily_partition_def,
    selection=[workspace_data_json],
    tags={
        "project": "aws-workspace-replication",
        "dagster/max_retries": 1,
    },
)


@dg.schedule(cron_schedule="0 3 * * *", job=aws_replication_job)
def aws_replication_schedule(context: dg.ScheduleEvaluationContext):
    date = (context.scheduled_execution_time.date() - timedelta(days=1)).strftime("%Y-%m-%d")
    all_orgs = sorted(
        org_partitions_def.get_partition_keys(dynamic_partitions_store=context.instance)
    )
    batches = [all_orgs[i : i + BATCH_SIZE] for i in range(0, len(all_orgs), BATCH_SIZE)]
    for i, batch in enumerate(batches):
        yield dg.RunRequest(
            run_key=f"{date}-batch-{i}",
            partition_key=date,
            run_config=RunConfig(
                ops={"workspace_data_json": WorkspaceReplicationConfig(org_ids=batch)}
            ),
        )


@definitions
def defs():
    return dg.Definitions(
        jobs=[aws_replication_job],
        schedules=[aws_replication_schedule],
    )
