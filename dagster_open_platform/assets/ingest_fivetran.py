from dagster import (
    AssetKey,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    EnvVar,
    load_assets_from_current_module,
)
from dagster_fivetran import FivetranResource, load_assets_from_fivetran_instance

fivetran_instance = FivetranResource(
    api_key=EnvVar("FIVETRAN_API_KEY"),
    api_secret=EnvVar("FIVETRAN_API_SECRET"),
)

fivetran_assets_no_amp = load_assets_from_fivetran_instance(
    fivetran_instance,
    connector_to_group_fn=lambda connector: f"fivetran_{connector}",
    connector_to_asset_key_fn=lambda metadata, table_name: AssetKey(
        ["fivetran", *table_name.split(".")]
    ),
)

fivetran_assets = load_assets_from_current_module(
    auto_materialize_policy=AutoMaterializePolicy.eager().with_rules(
        AutoMaterializeRule.materialize_on_cron("0 0 * * *")
    ),
)
