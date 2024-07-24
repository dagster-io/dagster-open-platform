from dagster import Definitions
from dagster_open_platform.scout.assets import github_issues
from dagster_open_platform.scout.resources import github_resource, scoutos_resource
from dagster_open_platform.scout.schedules import support_bot_job, support_bot_schedule

from ..utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git([github_issues]),
    jobs=[support_bot_job],
    schedules=[support_bot_schedule],
    resources={"github": github_resource, "scoutos": scoutos_resource},
)
