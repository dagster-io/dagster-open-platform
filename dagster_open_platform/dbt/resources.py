from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject
from dagster_open_platform.utils.environment_helpers import get_dbt_target

dagster_open_platform_dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "dagster_open_platform_dbt").resolve(),
    target=get_dbt_target(),
)

dbt_resource = DbtCliResource(project_dir=dagster_open_platform_dbt_project)
