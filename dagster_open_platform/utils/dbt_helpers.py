import os
from typing import Any, Mapping

from dagster import AssetKey, Config, MetadataValue
from dagster_dbt import DagsterDbtTranslator

from .environment_helpers import get_environment

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
    @classmethod
    def get_asset_key(cls, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        environment = get_environment()
        resource_type = dbt_resource_props["resource_type"]
        resource_name = dbt_resource_props["name"]

        if resource_type in ("model", "seed"):
            resource_database = dbt_resource_props["database"]
            schema = (
                str(cls.get_group_name(dbt_resource_props))
                if environment != "LOCAL"
                else os.getenv("SNOWFLAKE_USER", "")
            )
            resource_name = dbt_resource_props["name"]

            return AssetKey([resource_database, schema, resource_name])

        elif resource_type == "source":
            # if metadata has been provided in the yaml use that, otherwise construct key
            if (
                "meta" in dbt_resource_props
                and "dagster" in dbt_resource_props["meta"]
                and "asset_key" in dbt_resource_props["meta"]["dagster"]
            ):
                return AssetKey(dbt_resource_props["meta"]["dagster"]["asset_key"])

            else:
                database_name = dbt_resource_props["database"].lower()
                schema_name = dbt_resource_props["schema"].lower()
                return AssetKey([database_name, schema_name, resource_name])

        else:
            raise ValueError(f"Unknown dbt resource_type: {resource_type}")

    @classmethod
    def get_metadata(cls, dbt_node_info: Mapping[str, Any]) -> Mapping[str, Any]:
        if dbt_node_info["resource_type"] != "model":
            return {}

        return {
            "url": MetadataValue.url(
                f"{SNOWFLAKE_URL}/{dbt_node_info['schema'].upper()}/table/{dbt_node_info['name'].upper()}"
            )
        }
