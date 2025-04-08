from dagster import Definitions
from dagster_open_platform.defs.snowflake.assets.common_room import (
    common_room_aws_external_table,
    common_room_aws_stage,
)
from dagster_open_platform.defs.snowflake.assets.oss_telemetry import (
    oss_telemetry_aws_external_table,
    oss_telemetry_aws_stage,
)
from dagster_open_platform.defs.snowflake.assets.snowflake_clones import inactive_snowflake_clones
from dagster_open_platform.defs.snowflake.assets.user_roles import (
    user_roles_aws_external_table,
    user_roles_aws_stage,
)
from dagster_open_platform.defs.snowflake.assets.workspace_replication import (
    workspace_replication_aws_external_tables,
    workspace_replication_aws_stages,
)
from dagster_open_platform.defs.snowflake.jobs import clone_databases, drop_database_clones
from dagster_open_platform.defs.snowflake.resources import snowflake_resource
from dagster_open_platform.defs.snowflake.schedules import database_clone_cleanup_schedule
from dagster_open_platform.defs.snowflake.sensors import drop_old_database_clones
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

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
            common_room_aws_stage,
            common_room_aws_external_table,
        ]
    ),
    schedules=[database_clone_cleanup_schedule],
    resources={"snowflake_sf": snowflake_resource},
    jobs=[clone_databases, drop_database_clones],
    sensors=[drop_old_database_clones],
)
