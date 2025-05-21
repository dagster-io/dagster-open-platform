from collections.abc import Mapping
from functools import cached_property
from typing import Annotated, Any, Callable, Optional, Union

import dagster as dg
import dagster.components as dg_components
from dagster._core.definitions.asset_check_factories.schema_change_checks import BaseModel
from dagster.components import Component, ComponentLoadContext, Model, Resolvable
from dagster.components.resolved.base import resolve_fields
from dagster.components.utils import TranslatorResolvingInfo
from dagster_fivetran import (
    DagsterFivetranTranslator,
    FivetranConnectorTableProps,
    FivetranWorkspace,
    build_fivetran_assets_definitions,
)
from typing_extensions import TypeAlias


def resolve_translation(context: dg_components.ResolutionContext, model):
    info = TranslatorResolvingInfo(
        "props",
        asset_attributes=model,
        resolution_context=context,
        model_key="translation",
    )
    return lambda base_asset_spec, props: info.get_asset_spec(
        base_asset_spec,
        {
            "props": props,
            "spec": base_asset_spec,
        },
    )


TranslationFn: TypeAlias = Callable[[dg.AssetSpec, FivetranConnectorTableProps], dg.AssetSpec]
ResolvedTranslationFn: TypeAlias = Annotated[
    TranslationFn,
    dg_components.Resolver(
        resolve_translation,
        model_field_type=Union[str, dg_components.AssetAttributesModel],  # type: ignore
    ),
]


class ProxyDagsterFivetranTranslator(DagsterFivetranTranslator):
    def __init__(self, fn: TranslationFn):
        self.fn = fn

    def get_asset_spec(self, props: FivetranConnectorTableProps) -> dg.AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        spec = self.fn(base_asset_spec, props)

        return spec


class FivetranWorkspaceModel(BaseModel):
    api_key: str
    api_secret: str
    account_id: str


class FivetranComponent(Component, Model, Resolvable):
    """Loads Fivetran connectors as Dagster assets."""

    workspace: Annotated[
        FivetranWorkspace,
        dg_components.Resolver(
            lambda context, model: FivetranWorkspace(
                **resolve_fields(model, FivetranWorkspaceModel, context)  # type: ignore
            )
        ),
    ]
    translation: Optional[ResolvedTranslationFn] = None

    @cached_property
    def translator(self) -> DagsterFivetranTranslator:
        if self.translation:
            return ProxyDagsterFivetranTranslator(self.translation)
        return DagsterFivetranTranslator()

    @classmethod
    def get_additional_scope(cls) -> Mapping[str, Any]:
        return {
            "hourly_if_not_in_progress": dg.AutomationCondition.cron_tick_passed("0 * * * *")
            & ~dg.AutomationCondition.in_progress(),
            "group_from_db_and_schema": lambda props: f"fivetran_{'_'.join(props.table.split('.')[:-1])}",
        }

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        fivetran_assets = build_fivetran_assets_definitions(
            workspace=self.workspace,
            dagster_fivetran_translator=self.translator,
        )
        return dg.Definitions(assets=fivetran_assets, resources={"fivetran": self.workspace})
