import dagster as dg
from dagster.components import definitions
from dagster_fivetran import (
    DagsterFivetranTranslator,
    FivetranConnectorTableProps,
    FivetranWorkspace,
    build_fivetran_assets_definitions,
)


class DOPFivetranTranslator(DagsterFivetranTranslator):
    def get_asset_spec(self, props: FivetranConnectorTableProps) -> dg.AssetSpec:
        return (
            super()
            .get_asset_spec(props)
            .replace_attributes(
                key=dg.AssetKey(["fivetran", *props.table.split(".")]),
                automation_condition=dg.AutomationCondition.cron_tick_passed("0 * * * *")
                & ~dg.AutomationCondition.in_progress(),
                group_name=f"fivetran_{'_'.join(props.table.split('.')[:-1])}",
            )
        )


@definitions
def defs():
    fivetran_instance = FivetranWorkspace(
        account_id=dg.EnvVar("FIVETRAN_ACCOUNT_ID"),
        api_key=dg.EnvVar("FIVETRAN_API_KEY"),
        api_secret=dg.EnvVar("FIVETRAN_API_SECRET"),
    )

    fivetran_assets = build_fivetran_assets_definitions(
        workspace=fivetran_instance, dagster_fivetran_translator=DOPFivetranTranslator()
    )

    return dg.Definitions(assets=fivetran_assets, resources={"fivetran": fivetran_instance})
