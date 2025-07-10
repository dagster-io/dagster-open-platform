from functools import cache
from pathlib import Path

from dagster import Definitions
from dagster.components import definitions
from dagster_dbt import DbtCliResource, DbtProject
from dagster_open_platform.utils.environment_helpers import get_dbt_target


@cache
def dagster_open_platform_dbt_project() -> DbtProject:
    project = DbtProject(
        project_dir=Path(__file__)
        .joinpath("..", "..", "..", "..", "dagster_open_platform_dbt")
        .resolve(),
        target=get_dbt_target(),
    )
    project.prepare_if_dev()
    return project


@definitions
def defs():
    return Definitions(
        resources={"dbt": DbtCliResource(project_dir=dagster_open_platform_dbt_project())}
    )
