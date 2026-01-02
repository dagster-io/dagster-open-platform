import os
from collections.abc import Mapping
from typing import Any, Optional

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings, DbtProject

SNOWFLAKE_ACCOUNT_BASE, *_ = os.getenv("SNOWFLAKE_ACCOUNT", ".").split(".")
PURINA_DATABASE_NAME = (
    f"PURINA_CLONE_{os.environ['DAGSTER_CLOUD_PULL_REQUEST_ID']}"
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1"
    else "PURINA"
)
SNOWFLAKE_URL = f"https://app.snowflake.com/ax61354/{SNOWFLAKE_ACCOUNT_BASE}/#/data/databases/{PURINA_DATABASE_NAME}/schemas"

asset_is_new_or_updated = ~dg.AutomationCondition.in_progress() & (
    dg.AutomationCondition.code_version_changed() | dg.AutomationCondition.missing()
)


asset_is_new_or_updated_or_deps_updated = ~dg.AutomationCondition.in_progress() & (
    dg.AutomationCondition.code_version_changed()
    | dg.AutomationCondition.missing()
    | dg.AutomationCondition.any_deps_updated()
)


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def _get_group_name_for_resource(self, dbt_props: Mapping[str, Any]) -> str:
        """Calculate the group name based on dbt resource properties.

        Snapshots go to "snapshots" group, other models use their directory path
        (same logic as macros/get_custom_schema.sql).
        """
        resource_type = dbt_props["resource_type"]
        if resource_type == "snapshot":
            return "snapshots"

        # fqn[2:-1] skips project name, model type, and asset name
        fqn = dbt_props.get("fqn", [])
        asset_path = fqn[2:-1]
        if asset_path:
            return "_".join(asset_path)
        return "default"

    def _get_asset_key_for_resource(self, dbt_props: Mapping[str, Any]) -> dg.AssetKey:
        """Calculate the asset key based on dbt resource properties.

        For sources with meta.dagster.asset_key, use the custom key.
        Otherwise, use the default 3-part key: [database, schema, name].
        """
        resource_type = dbt_props["resource_type"]
        meta = dbt_props.get("meta", {})
        dagster_meta = meta.get("dagster", {})

        # Check for custom asset key in source metadata
        if "asset_key" in dagster_meta and resource_type == "source":
            return dg.AssetKey(dagster_meta["asset_key"])

        # Default 3-part key
        return dg.AssetKey(
            [
                dbt_props["database"],
                dbt_props["schema"],
                dbt_props["name"],
            ]
        )

    def _get_metadata_for_resource(self, dbt_props: Mapping[str, Any]) -> Mapping[str, Any]:
        """Calculate custom metadata for dbt resource.

        For models, adds a Snowflake Data Explorer URL.
        """
        resource_type = dbt_props["resource_type"]
        if resource_type != "model":
            return {}

        # Construct Snowflake URL with uppercase schema and name
        schema = dbt_props["schema"].upper()
        name = dbt_props["name"].upper()
        url = f"{SNOWFLAKE_URL}/{schema}/table/{name}"
        return {"dagster/uri": dg.MetadataValue.url(url)}

    def _get_automation_condition_for_resource(
        self, dbt_props: Mapping[str, Any]
    ) -> dg.AutomationCondition:
        """Calculate automation condition based on database.

        dwh_reporting uses stricter condition (includes dependency updates).
        Other databases use standard condition (code changes or missing only).
        """
        database = dbt_props["database"]
        if database.lower() == "dwh_reporting":
            return asset_is_new_or_updated_or_deps_updated
        return asset_is_new_or_updated

    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: Optional[DbtProject],
    ) -> dg.AssetSpec:
        """Build the asset spec by applying all customizations via helper methods."""
        base_spec = super().get_asset_spec(manifest, unique_id, project)
        dbt_props = self.get_resource_props(manifest, unique_id)

        # Apply customizations using helper methods
        group_name = self._get_group_name_for_resource(dbt_props)
        asset_key = self._get_asset_key_for_resource(dbt_props)
        metadata = self._get_metadata_for_resource(dbt_props)
        automation_condition = self._get_automation_condition_for_resource(dbt_props)

        # Return modified spec
        return base_spec.replace_attributes(
            key=asset_key,
            group_name=group_name,
            automation_condition=automation_condition,
        ).merge_attributes(metadata=metadata)


class RegionPrefixedDbtTranslator(CustomDagsterDbtTranslator):
    """Adds the region to the start of the default AssetKey."""

    def __init__(
        self,
        settings: DagsterDbtTranslatorSettings,
        asset_key_prefix: str,
    ):
        super().__init__(settings=settings)
        self.asset_key_prefix = asset_key_prefix

    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: Optional[DbtProject],
    ) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(manifest, unique_id, project)

        # Add region prefix to the asset key using with_prefix()
        prefixed_key = base_spec.key.with_prefix(self.asset_key_prefix)

        return base_spec.replace_attributes(key=prefixed_key)
