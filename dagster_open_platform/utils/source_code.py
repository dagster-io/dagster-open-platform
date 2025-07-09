# copied from dagster-cloud/python_modules/dagster-cloud/dagster_cloud/metadata/source_code.py
# in advance of releasing this functionality

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Union

from dagster import AssetsDefinition, AssetSpec, Definitions
from dagster._core.definitions.metadata import (
    AnchorBasedFilePathMapping,
    os,
    with_source_code_references,
)
from dagster_cloud.metadata.source_code import link_code_references_to_git_if_cloud

REPOSITORY_ROOT_ABSOLUTE_PATH = (
    Path(__file__).parent.parent.parent.parent.parent.resolve().absolute()
)


if TYPE_CHECKING:
    from dagster import SourceAsset
    from dagster._core.definitions.definitions_class import CacheableAssetsDefinition


def add_code_references_and_link_to_git(
    assets: Sequence[
        Union["AssetsDefinition", "SourceAsset", "CacheableAssetsDefinition", "AssetSpec"]
    ],
) -> Sequence[Union["AssetsDefinition", "SourceAsset", "CacheableAssetsDefinition", "AssetSpec"]]:
    return link_code_references_to_git_if_cloud(
        with_source_code_references(assets),
        file_path_mapping=AnchorBasedFilePathMapping(
            local_file_anchor=REPOSITORY_ROOT_ABSOLUTE_PATH, file_anchor_path_in_repository="."
        ),
    )


def link_defs_code_references_to_git(
    defs: Definitions,
) -> Definitions:
    is_dagster_cloud = os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME") is not None

    if not is_dagster_cloud:
        return defs

    return Definitions(
        assets=link_code_references_to_git_if_cloud(
            list(defs.assets),
            file_path_mapping=AnchorBasedFilePathMapping(
                local_file_anchor=REPOSITORY_ROOT_ABSOLUTE_PATH, file_anchor_path_in_repository="."
            ),
        )
        if defs.assets
        else None,
        schedules=defs.schedules,
        sensors=defs.sensors,
        jobs=defs.jobs,
        resources=defs.resources,
        executor=defs.executor,
        loggers=defs.loggers,
        asset_checks=defs.asset_checks,
        metadata=defs.metadata,
    )
