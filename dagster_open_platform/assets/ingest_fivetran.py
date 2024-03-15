from dagster import (
    AssetKey,
    EnvVar,
)
from dagster_fivetran import (
    FivetranResource,
    load_assets_from_fivetran_instance,
)

fivetran_instance = FivetranResource(
    api_key=EnvVar("FIVETRAN_API_KEY"),
    api_secret=EnvVar("FIVETRAN_API_SECRET"),
)

fivetran_assets = load_assets_from_fivetran_instance(
    fivetran_instance,
    connector_to_group_fn=lambda connector: f"fivetran_{connector}",
    connector_to_asset_key_fn=lambda metadata, table_name: AssetKey(
        ["fivetran", *table_name.split(".")]
    ),
)
