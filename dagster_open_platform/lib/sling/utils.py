from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, Optional

import dagster_shared.check as check
from dagster import AssetKey, AssetSpec
from dagster.components import ResolutionContext
from dagster_dbt import get_asset_key_for_model
from dagster_sling import DagsterSlingTranslator, SlingResource, sling_assets


def build_sling_assets(
    name: str,
    config_path: Path,
    build_deps: bool = True,
    group_name: str | None = None,
    translator: Optional[DagsterSlingTranslator] = None,
):
    @sling_assets(
        name=name,
        replication_config=str(config_path),
        dagster_sling_translator=translator,
    )
    def _sling_assets(context, embedded_elt: SlingResource) -> Iterable[Any]:
        yield from (
            embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()
        )

    asset_specs: Sequence[AssetSpec] = (
        [
            AssetSpec(key, group_name=group_name)
            for key in getattr(_sling_assets, "dependency_keys", [])
        ]
        if build_deps
        else []
    )

    return _sling_assets, asset_specs


def _resolve_path(context: ResolutionContext, val: str):
    p = context.resolve_source_relative_path(val)
    check.invariant(p.exists(), f"resolved config_dir Path {p} does not exist")
    return p


def dbt_asset_key(model_name: str) -> AssetKey:
    from dagster_open_platform.defs.dbt.assets import get_dbt_non_partitioned_models

    return get_asset_key_for_model([get_dbt_non_partitioned_models()], model_name)
