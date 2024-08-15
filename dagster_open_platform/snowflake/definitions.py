from dagster import Definitions
from dagster_open_platform.snowflake.assets import aws_stages, inactive_snowflake_clones
from dagster_open_platform.snowflake.resources import snowflake_resource
from dagster_open_platform.snowflake.schedules import purina_clone_cleanup_schedule

from ..utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git([inactive_snowflake_clones, aws_stages]),
    schedules=[
        purina_clone_cleanup_schedule,
    ],
    resources={"snowflake_sf": snowflake_resource},
)
