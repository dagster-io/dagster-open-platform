import dagster as dg
from dagster_open_platform.aws.assets import workspace_data_json
from dagster_open_platform.snowflake.assets import (
    user_roles_aws_external_table,
    user_roles_aws_stage,
    workspace_replication_aws_external_tables,
    workspace_replication_aws_stages,
)

aws_replication_schedule = dg.ScheduleDefinition(
    name="aws_replication_schedule",
    cron_schedule="0 3 * * *",
    job=dg.define_asset_job(
        name="aws_replication_job",
        selection=[
            workspace_data_json,
            workspace_replication_aws_external_tables,
            workspace_replication_aws_stages,
            user_roles_aws_external_table,
            user_roles_aws_stage,
        ],
        tags={"project": "aws-workspace-replication"},
    ),
)
