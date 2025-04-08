import dagster as dg
from dagster_fivetran import FivetranResource, load_assets_from_fivetran_instance

fivetran_instance = FivetranResource(
    api_key=dg.EnvVar("FIVETRAN_API_KEY"),
    api_secret=dg.EnvVar("FIVETRAN_API_SECRET"),
)

fivetran_assets_no_amp = load_assets_from_fivetran_instance(
    fivetran_instance,
    connector_to_group_fn=lambda connector: f"fivetran_{'_'.join(connector.split('.'))}",
    connector_to_asset_key_fn=lambda metadata, table_name: dg.AssetKey(
        ["fivetran", *table_name.split(".")]
    ),
)

fivetran_assets = dg.load_assets_from_current_module(
    automation_condition=dg.AutomationCondition.cron_tick_passed("0 * * * *")
    & ~dg.AutomationCondition.in_progress(),
)
