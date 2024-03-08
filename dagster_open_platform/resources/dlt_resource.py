from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

import dlt
from dagster import (
    AssetKey,
    AssetSpec,
    ConfigurableResource,
    MaterializeResult,
    ResourceDependency,
    multi_asset,
)
from dagster._annotations import experimental, public
from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource
from pydantic import Field

DLT_INCLUDED_METADATA = {
    "first_run",
    "started_at",
    "finished_at",
    "dataset_name",
    "destination_name",
    "destination_type",
}


@experimental
class ConfigurableDltResource(ConfigurableResource):
    source: ResourceDependency[DltSource]
    pipeline_name: str = Field(description="pipeline name parameter of `dlt.pipline()`")
    dataset_name: str = Field(description="dataset name parameter of `dlt.pipeline()`")
    destination: str = Field(
        description="target destination parameter of `dlt.pipeline()` (eg. duckdb, snowflake)"
    )

    def _initialize_pipeline(self, **kwargs):
        return dlt.pipeline(
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            destination=self.destination,
            **kwargs,
        )

    def run(self, **pipeline_kwargs):
        pipeline = self._initialize_pipeline(**pipeline_kwargs)
        return pipeline.run(self.source)


@dataclass
class DagsterDltTranslator:
    @classmethod
    @public
    def get_metadata(cls, asset_key: str, load_info: Mapping[str, Any]) -> Mapping[str, Any]:
        # TODO - filter by `asset_key`
        return {k: str(v) for k, v in load_info.items() if k in DLT_INCLUDED_METADATA}

    @classmethod
    @public
    def get_asset_key(cls, resource_key: str, dataset_name: str) -> AssetKey:
        """Defines asset key for a given dlt resource key and dataset name.

        Args:
            resource_key (str): key of dlt resource
            dataset_name (str): name of dlt dataset

        Returns:
            AssetKey of Dagster asset derived from dlt resource

        """
        return AssetKey(f"dlt_{dataset_name}_{resource_key}")

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


def build_dlt_assets(
    dlt_pipeline_resource: ConfigurableDltResource,
    name: Optional[str] = None,
    group_name: Optional[str] = None,
    dagster_dlt_translator: DagsterDltTranslator = DagsterDltTranslator(),
):
    specs = [
        AssetSpec(
            key=dagster_dlt_translator.get_asset_key(key, dlt_pipeline_resource.dataset_name),
            deps=dagster_dlt_translator.get_deps_asset_keys(value),
        )
        for key, value in dlt_pipeline_resource.source.resources.items()
    ]

    @multi_asset(
        name=name,
        specs=specs,
        group_name=group_name,
        compute_kind="dlt",
    )
    def _assets():
        load_info = dlt_pipeline_resource.run()

        for spec in specs:
            metadata = dagster_dlt_translator.get_metadata(str(spec.key), load_info.asdict())
            yield MaterializeResult(asset_key=spec.key, metadata=metadata)

    return [_assets]
