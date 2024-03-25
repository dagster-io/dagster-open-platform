import os
from typing import Any, Mapping, Optional

from dagster import AssetKey, Config, MetadataValue
from dagster_dbt import DagsterDbtTranslator

SNOWFLAKE_ACCOUNT_BASE = os.getenv("SNOWFLAKE_ACCOUNT", ".").split(".")[0]
PURINA_DATABASE_NAME = (
    f"PURINA_CLONE_{os.environ['DAGSTER_CLOUD_PULL_REQUEST_ID']}"
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1"
    else "PURINA"
)
SNOWFLAKE_URL = f"https://app.snowflake.com/ax61354/{SNOWFLAKE_ACCOUNT_BASE}/#/data/databases/{PURINA_DATABASE_NAME}/schemas"


class DbtConfig(Config):
    full_refresh: bool = False


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        # Same logic that sets the custom schema in macros/get_custom_schema.sql
        asset_path = dbt_resource_props["fqn"][1:-1]
        if asset_path:
            return "_".join(asset_path)
        return "default"

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_database = dbt_resource_props["database"]
        resource_schema = dbt_resource_props["schema"]
        resource_name = dbt_resource_props["name"]

        return AssetKey([resource_database, resource_schema, resource_name])

    def get_metadata(self, dbt_node_info: Mapping[str, Any]) -> Mapping[str, Any]:
        if dbt_node_info["resource_type"] != "model":
            return {}

        return {
            "url": MetadataValue.url(
                f"{SNOWFLAKE_URL}/{dbt_node_info['schema'].upper()}/table/{dbt_node_info['name'].upper()}"
            )
        }
