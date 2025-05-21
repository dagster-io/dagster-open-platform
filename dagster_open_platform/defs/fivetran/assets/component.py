import os

import dagster as dg
from dagster.components import ComponentLoadContext, component
from dagster_fivetran import FivetranConnectorTableProps, FivetranWorkspace
from dagster_open_platform.defs.fivetran.assets.fivetran_component_type import FivetranComponent


@component
def load(context: ComponentLoadContext) -> FivetranComponent:
    def _translate(
        base_asset_spec: dg.AssetSpec, props: FivetranConnectorTableProps
    ) -> dg.AssetSpec:
        return base_asset_spec.replace_attributes(
            key=dg.AssetKey(["fivetran", *props.table.split(".")]),
            automation_condition=dg.AutomationCondition.cron_tick_passed("0 * * * *")
            & ~dg.AutomationCondition.in_progress(),
            group_name=f"fivetran_{'_'.join(props.table.split('.')[:-1])}",
        )

    return FivetranComponent(
        workspace=FivetranWorkspace(
            account_id=os.environ["FIVETRAN_ACCOUNT_ID"],
            api_key=os.environ["FIVETRAN_API_KEY"],
            api_secret=os.environ["FIVETRAN_API_SECRET"],
        ),
        translation=_translate,
    )
