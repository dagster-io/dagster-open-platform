import dagster as dg
from dagster.components import definitions
from dagster_fivetran import FivetranResource, load_assets_from_fivetran_instance


@definitions
def defs():
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

    fivetran_assets = fivetran_assets_no_amp.with_attributes_for_all(
        group_name=None,
        freshness_policy=None,
        backfill_policy=None,
        auto_materialize_policy=(
            dg.AutomationCondition.cron_tick_passed("0 * * * *")
            & ~dg.AutomationCondition.in_progress()
        ).as_auto_materialize_policy(),
    )

    return dg.Definitions(
        assets=[
            fivetran_assets,
        ]
    )
