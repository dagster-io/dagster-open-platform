from dagster import Definitions
from dagster_open_platform.aws.assets import aws_cost_report, workspace_data_json
from dagster_open_platform.aws.resources import s3_resource
from dagster_open_platform.aws.schedules import aws_replication_schedule
from dagster_open_platform.aws.sensors import organization_sensor
from dagster_open_platform.snowflake.resources import snowflake_resource

from ..utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git([aws_cost_report, workspace_data_json]),
    sensors=[organization_sensor],
    resources={
        "snowflake_aws": snowflake_resource,
        "s3_resource": s3_resource,
    },
    schedules=[aws_replication_schedule],
)
