import os

from dagster import file_relative_path
from dagster_dbt import DbtCliResource


def get_target():
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "") == "1":
        return "branch_deployment"
    if os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "") == "prod":
        return "prod"
    if os.getenv("CI"):
        return "test"
    return os.getenv("DBT_TARGET")


dbt_resource = DbtCliResource(
    project_dir=file_relative_path(__file__, "../../purina_dbt"),
    profiles_dir=file_relative_path(__file__, "../../purina_dbt"),
    target=get_target(),
)
DBT_MANIFEST_PATH = file_relative_path(__file__, "../../purina_dbt/target/manifest.json")
