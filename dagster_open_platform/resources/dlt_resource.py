from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Union

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
from dlt.common.pipeline import LoadInfo
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
    def _cast_load_info_metadata(self, mapping: Mapping[Any, Any]) -> Mapping[Any, Any]:
        """Converts pendulum DateTime and Timezone values in a mapping to strings.

        Workaround for dagster._core.errors.DagsterInvalidMetadata: Could not resolve the
        metadata value for "jobs" to a known type. Value is not JSON serializable.

        Args:
            mapping (Mapping): dictionary possibly containing pendulum values

        Returns:
            mapping with pendulum DateTime and Timezone values casted to strings

        """
        from pendulum import DateTime, Timezone

        def _recursive_cast(value):
            if isinstance(value, dict):
                return {k: _recursive_cast(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [_recursive_cast(item) for item in value]
            elif isinstance(value, (DateTime, Timezone)):
                return str(value)
            else:
                return value

        return {k: _recursive_cast(v) for k, v in mapping.items()}

    def extract_resource_metadata(
        self, resource: DltResource, load_info: LoadInfo
    ) -> Mapping[str, Any]:
        """Helper method to extract dlt resource metadata from load info dict.

        Args:
            resource (DltResource) dlt resource being materialized
            load_info (LoadInfo) run metadata from dlt pipeline.run(...)

        Returns:
            Mapping[str, Any]: asset-specific metadata dictionary

        """
        dlt_base_metadata_types = {
            "first_run",
            "started_at",
            "finished_at",
            "dataset_name",
            "destination_name",
            "destination_type",
        }

        load_info_dict = self._cast_load_info_metadata(load_info.asdict())

        # shared metadata that is displayed for all assets
        base_metadata = {k: v for k, v in load_info_dict.items() if k in dlt_base_metadata_types}

        # job metadata for specific target `resource.table_name`
        base_metadata["jobs"] = [
            job
            for load_package in load_info_dict.get("load_packages", [])
            for job in load_package.get("jobs", [])
            if job.get("table_name") == resource.table_name
        ]

        return base_metadata

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

        load_info = dlt_pipeline.run(dlt_source)

        # Enhancement: ensure `zip` arrays align; support sub-selections [?]
        for asset_key, dlt_source_resource in zip(
            context.selected_asset_keys, dlt_source.resources.values()
        ):
            metadata = self.extract_resource_metadata(dlt_source_resource, load_info)

            if isinstance(context, AssetExecutionContext):
                yield MaterializeResult(asset_key=asset_key, metadata=metadata)

            else:
                yield AssetMaterialization(asset_key=asset_key, metadata=metadata)


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
