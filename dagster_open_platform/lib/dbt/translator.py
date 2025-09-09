import os
from collections.abc import Mapping
from typing import Any, Optional

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings

SNOWFLAKE_ACCOUNT_BASE, *_ = os.getenv("SNOWFLAKE_ACCOUNT", ".").split(".")
PURINA_DATABASE_NAME = (
    f"PURINA_CLONE_{os.environ['DAGSTER_CLOUD_PULL_REQUEST_ID']}"
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1"
    else "PURINA"
)
SNOWFLAKE_URL = f"https://app.snowflake.com/ax61354/{SNOWFLAKE_ACCOUNT_BASE}/#/data/databases/{PURINA_DATABASE_NAME}/schemas"


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        if dbt_resource_props["resource_type"] == "snapshot":
            return "snapshots"
        # Same logic that sets the custom schema in macros/get_custom_schema.sql
        asset_path = dbt_resource_props["fqn"][2:-1]
        if asset_path:
            return "_".join(asset_path)
        return "default"

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        resource_database = dbt_resource_props["database"]
        resource_schema = dbt_resource_props["schema"]
        resource_name = dbt_resource_props["name"]
        resource_type = dbt_resource_props["resource_type"]

        # if metadata has been provided in the yaml use that, otherwise construct key
        if (
            resource_type == "source"
            and "meta" in dbt_resource_props
            and "dagster" in dbt_resource_props["meta"]
            and "asset_key" in dbt_resource_props["meta"]["dagster"]
        ):
            return dg.AssetKey(dbt_resource_props["meta"]["dagster"]["asset_key"])

        return dg.AssetKey([resource_database, resource_schema, resource_name])

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        url_metadata = {}
        if dbt_resource_props["resource_type"] == "model":
            url_metadata = {
                "url": dg.MetadataValue.url(
                    "/".join(
                        [
                            SNOWFLAKE_URL,
                            dbt_resource_props["schema"].upper(),
                            "table",
                            dbt_resource_props["name"].upper(),
                        ]
                    )
                )
            }

        return {
            **super().get_metadata(dbt_resource_props),
            **url_metadata,
        }

    def get_automation_condition(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Optional[dg.AutomationCondition]:
        return ~dg.AutomationCondition.in_progress() & dg.AutomationCondition.code_version_changed()


class RegionPrefixedDbtTranslator(CustomDagsterDbtTranslator):
    """Adds the region to the start of the default AssetKey."""

    def __init__(
        self,
        settings: DagsterDbtTranslatorSettings,
        asset_key_prefix: str,
    ):
        super().__init__(settings=settings)
        self.asset_key_prefix = asset_key_prefix

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        return super().get_asset_key(dbt_resource_props).with_prefix(self.asset_key_prefix)
