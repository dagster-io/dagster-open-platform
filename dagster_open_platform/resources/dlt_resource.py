from dataclasses import dataclass
from typing import Callable, Iterable, Iterator, Optional, Union

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetsDefinition,
    AssetSpec,
    AutoMaterializePolicy,
    ConfigurableResource,
    MaterializeResult,
    OpExecutionContext,
    multi_asset,
)
from dagster._annotations import public
from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource
from dlt.pipeline.pipeline import Pipeline

META_KEY_SOURCE = "dagster_dlt/source"
META_KEY_PIPELINE = "dagster_dlt/pipeline"


@dataclass
class DltDagsterTranslator:
    @classmethod
    @public
    def get_asset_key(cls, resource: DltResource) -> AssetKey:
        """Defines asset key for a given dlt resource key and dataset name.

        Args:
            resource_key (str): key of dlt resource
            dataset_name (str): name of dlt dataset

        Returns:
            AssetKey of Dagster asset derived from dlt resource

        """
        return AssetKey(f"dlt_{resource.source_name}_{resource.name}")

    @classmethod
    @public
    def get_deps_asset_keys(cls, resource: DltResource) -> Iterable[AssetKey]:
        """Defines upstream asset dependencies given a dlt resource.

        Defaults to a concatenation of `resource.source_name` and `resource.name`.

        Args:
            resource (DltResource): dlt resource / transformer

        Returns:
            Iterable[AssetKey]: The Dagster asset keys upstream of `dlt_resource_key`.

        """
        return [AssetKey(f"{resource.source_name}_{resource.name}")]

    @classmethod
    @public
    def get_auto_materialize_policy(cls, resource: DltResource) -> Optional[AutoMaterializePolicy]:
        """Defines resource specific auto materialize policy.

        Args:
            resource (DltResource): dlt resource / transformer

        Returns:
            Optional[AutoMaterializePolicy]: The automaterialize policy for a resource

        """
        return None


class DltDagsterResource(ConfigurableResource):
    @public
    def run(
        self,
        context: Union[OpExecutionContext, AssetExecutionContext],
    ) -> Iterator[Union[AssetMaterialization, MaterializeResult]]:
        metadata_by_key = context.assets_def.metadata_by_key
        first_asset_metadata = next(iter(metadata_by_key.values()))

        dlt_source = first_asset_metadata.get(META_KEY_SOURCE)
        dlt_pipeline = first_asset_metadata.get(META_KEY_PIPELINE)

        if not dlt_source or not dlt_pipeline:
            raise Exception(
                "%s and %s must be defined on AssetSpec metadata",
                META_KEY_SOURCE,
                META_KEY_PIPELINE,
            )

        dlt_pipeline.run(dlt_source)

        for (asset_key,) in context.selected_asset_keys:
            if isinstance(context, AssetExecutionContext):
                yield MaterializeResult(asset_key=asset_key)

            else:
                yield AssetMaterialization(asset_key=asset_key)


def dlt_assets(
    *,
    dlt_source: DltSource,
    dlt_pipeline: Pipeline,
    name: Optional[str] = None,
    group_name: Optional[str] = None,
    dlt_dagster_translator: DltDagsterTranslator = DltDagsterTranslator(),
) -> Callable[..., AssetsDefinition]:
    def inner(fn) -> AssetsDefinition:
        specs = [
            AssetSpec(
                key=dlt_dagster_translator.get_asset_key(dlt_source_resource),
                deps=dlt_dagster_translator.get_deps_asset_keys(dlt_source_resource),
                auto_materialize_policy=dlt_dagster_translator.get_auto_materialize_policy(
                    dlt_source_resource
                ),
                metadata={  # type: ignore - source / translator are included in metadata for use in `.run()`
                    META_KEY_SOURCE: dlt_source,
                    META_KEY_PIPELINE: dlt_pipeline,
                },
            )
            for dlt_source_resource in dlt_source.resources.values()
        ]
        assets_definition = multi_asset(
            specs=specs,
            name=name,
            group_name=group_name,
            compute_kind="dlt",
        )(fn)
        return assets_definition

    return inner
