from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets

from ..resources import (
    DBT_MANIFEST_PATH,
    dbt_resource,
)
from ..utils.dbt import CustomDagsterDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    select="source:postgres_etl_high_freq+ source:postgres_etl_low_freq+",
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def cloud_analytics_dbt_assets(context: AssetExecutionContext):
    yield from dbt_resource.cli(["build"], context=context).stream()
