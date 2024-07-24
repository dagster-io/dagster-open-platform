from dagster import Definitions, load_assets_from_modules
from dagster_open_platform.pypi import assets
from dagster_open_platform.pypi.resources import bigquery_resource
from dagster_open_platform.pypi.schedules import oss_analytics_schedule
from dagster_open_platform.snowflake.resources import snowflake_resource

from ..utils.source_code import add_code_references_and_link_to_git

pypi_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=add_code_references_and_link_to_git(pypi_assets),
    schedules=[oss_analytics_schedule],
    resources={"bigquery": bigquery_resource, "snowflake_pypi": snowflake_resource},
)
