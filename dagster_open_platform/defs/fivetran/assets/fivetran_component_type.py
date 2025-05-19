from functools import cached_property
from typing import Annotated, Callable, Optional, Union

import dagster as dg
import dagster.components as dg_components
from dagster._core.definitions.asset_check_factories.schema_change_checks import BaseModel
from dagster.components import Component, ComponentLoadContext, Model, Resolvable
from dagster.components.resolved.base import resolve_fields
from dagster.components.utils import TranslatorResolvingInfo
from dagster_fivetran import (
    DagsterFivetranTranslator,
    FivetranConnectorTableProps,
    FivetranResource,
    load_assets_from_fivetran_instance,
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


def build_proxy_translator(fn: TranslationFn) -> type[DagsterFivetranTranslator]:
    """Fivetran expects a translator to be a class, so we need to construct a custom proxy class rather than
    an instance of the DagsterFivetranTranslator class.
    """

    class ProxyDagsterFivetranTranslator(DagsterFivetranTranslator):
        def get_asset_spec(self, props: FivetranConnectorTableProps) -> dg.AssetSpec:
            base_asset_spec = super().get_asset_spec(props)
            spec = fn(base_asset_spec, props)

            return spec

    return ProxyDagsterFivetranTranslator


class FivetranResourceModel(BaseModel):
    api_key: str
    api_secret: str


class FivetranComponent(Component, Model, Resolvable):
    """Loads Fivetran connectors as Dagster assets."""

    instance: Annotated[
        FivetranResource,
        dg_components.Resolver(
            lambda context, model: FivetranResource(
                **resolve_fields(model, FivetranResourceModel, context)  # type: ignore
            )
        ),
    ]
    translation: Optional[ResolvedTranslationFn] = None

    @cached_property
    def translator_cls(self):
        if self.translation:
            return build_proxy_translator(self.translation)
        return DagsterFivetranTranslator

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        fivetran_assets = load_assets_from_fivetran_instance(
            self.instance,
            translator=self.translator_cls,
        )
        # Hackery - Fivetran translator doesn't support setting a group name on specs
        # because it sets it on the multi_asset directly as well, so we instead layer on
        # the group names after multi_asset is built.
        # https://github.com/dagster-io/dagster/pull/29968
        defs = [
            asset.map_asset_specs(
                lambda spec: spec.replace_attributes(
                    group_name=spec.metadata["group_name"],
                )
            )
            for asset in fivetran_assets.build_definitions(fivetran_assets.compute_cacheable_data())
        ]

        return dg.Definitions(assets=defs)
