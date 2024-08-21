from dagster import Definitions
from dagster_open_platform.snowflake.assets import (
    aws_external_tables,
    aws_stages,
    inactive_snowflake_clones,
)
from dagster_open_platform.snowflake.resources import snowflake_resource
from dagster_open_platform.snowflake.schedules import (
    purina_clone_cleanup_schedule,
    workspace_replication_snowflake_staging_schedule,
)

from ..utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git(
        [inactive_snowflake_clones, aws_stages, aws_external_tables]
    ),
    schedules=[purina_clone_cleanup_schedule, workspace_replication_snowflake_staging_schedule],
    resources={"snowflake_sf": snowflake_resource},
)
