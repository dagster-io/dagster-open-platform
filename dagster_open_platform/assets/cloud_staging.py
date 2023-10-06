from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets

from ..resources import (
    DBT_MANIFEST_PATH,
    dbt_resource,
)
from ..utils.dbt import PurinaDagsterDbtTranslator


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    select="purina_dbt.staging.*",
    dagster_dbt_translator=PurinaDagsterDbtTranslator(),
)
def cloud_analytics_dbt_assets(context: AssetExecutionContext):
    yield from dbt_resource.cli(["build"], context=context).stream()
