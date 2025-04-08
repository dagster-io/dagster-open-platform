from dagster import Definitions, load_assets_from_modules
from dagster_open_platform.defs.pypi import assets
from dagster_open_platform.defs.pypi.resources import bigquery_resource
from dagster_open_platform.defs.snowflake.resources import snowflake_resource
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

pypi_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=add_code_references_and_link_to_git(pypi_assets),
    resources={"bigquery": bigquery_resource, "snowflake_pypi": snowflake_resource},
)
