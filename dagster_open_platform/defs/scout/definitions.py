from dagster import Definitions
from dagster_open_platform.defs.scout.assets import github_issues, scoutos_app_runs
from dagster_open_platform.defs.scout.resources import github_resource, scoutos_resource
from dagster_open_platform.defs.scout.schedules import support_bot_job, support_bot_schedule
from dagster_open_platform.defs.snowflake.resources import snowflake_resource
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git([github_issues, scoutos_app_runs]),
    jobs=[support_bot_job],
    schedules=[support_bot_schedule],
    resources={
        "github": github_resource,
        "scoutos": scoutos_resource,
        "snowflake": snowflake_resource,
    },
)
