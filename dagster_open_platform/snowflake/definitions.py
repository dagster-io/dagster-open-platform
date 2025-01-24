from dagster import Definitions
from dagster_open_platform.snowflake.assets import (
    inactive_snowflake_clones,
    oss_telemetry_aws_external_table,
    oss_telemetry_aws_stage,
    user_roles_aws_external_table,
    user_roles_aws_stage,
    workspace_replication_aws_external_tables,
    workspace_replication_aws_stages,
)
from dagster_open_platform.snowflake.resources import snowflake_resource
from dagster_open_platform.snowflake.schedules import purina_clone_cleanup_schedule

from ..utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git(
        [
            inactive_snowflake_clones,
            workspace_replication_aws_stages,
            workspace_replication_aws_external_tables,
            user_roles_aws_stage,
            user_roles_aws_external_table,
            oss_telemetry_aws_stage,
            oss_telemetry_aws_external_table,
        ]
    ),
    schedules=[purina_clone_cleanup_schedule],
    resources={"snowflake_sf": snowflake_resource},
)
