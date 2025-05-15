import dagster as dg
from dagster.components import ComponentLoadContext, component
from dagster_dlt.asset_decorator import DltResourceTranslatorData
from dagster_dlt.components.dlt_load_collection.component import DltLoadSpecModel
from dagster_open_platform.defs.dlt.custom_component import CustomDltLoadCollectionComponent
from dagster_open_platform.defs.dlt.loads import thinkific_pipeline, thinkific_source

DLT_AUTOMATION_CONDITION = (
    dg.AutomationCondition.cron_tick_passed("0 0 * * *") & ~dg.AutomationCondition.in_progress()
)


def _set_asset_key_to_legacy(spec: dg.AssetSpec, data: DltResourceTranslatorData):
    """Replicates the non-component dagster_dlt asset key generation logic (which is less nice, but
    matches what we had previously).
    """
    return spec.replace_attributes(
        key=dg.AssetKey(["dlt_" + data.resource.source_name + "_" + data.resource.name]),
    )


@component
def load(context: ComponentLoadContext) -> CustomDltLoadCollectionComponent:
    return CustomDltLoadCollectionComponent(
        loads=[
            DltLoadSpecModel(
                pipeline=thinkific_pipeline,
                source=thinkific_source,
                translation=lambda spec, data: _set_asset_key_to_legacy(
                    spec.replace_attributes(
                        automation_condition=DLT_AUTOMATION_CONDITION,
                        group_name="thinkific",
                    ),
                    data,
                ),
            ),
        ]
    )
