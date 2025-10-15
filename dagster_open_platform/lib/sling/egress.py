from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any, Optional

import dagster_shared.check as check
from dagster import AssetKey, Definitions
from dagster.components import Component, ComponentLoadContext, Model, Resolvable, Resolver
from dagster.components.resolved.core_models import ResolvedAssetKey
from dagster_open_platform.lib.sling.utils import _resolve_path, build_sling_assets, dbt_asset_key
from dagster_sling import DagsterSlingTranslator


class EgressReplicationSlingTranslator(DagsterSlingTranslator):
    def __init__(self, deps: Sequence[AssetKey], asset_key_prefix: Optional[str] = None):
        super().__init__()
        self.deps = deps
        self.asset_key_prefix = asset_key_prefix

    def get_tags(self, stream_definition: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"dagster/kind/snowflake": ""}

    def get_metadata(self, stream_definition: Mapping[str, Any]) -> Mapping[str, Any]:
        key: AssetKey = self.get_asset_key(stream_definition)
        return {
            **super().get_metadata(stream_definition),
            "dagster/table_name": ".".join(key.path),
        }

    def get_group_name(self, stream_definition):
        return "sling_egress"

    def get_asset_key(self, stream_definition: Mapping[str, Any]) -> AssetKey:
        asset_key = super().get_asset_key(stream_definition)
        return asset_key.with_prefix(self.asset_key_prefix) if self.asset_key_prefix else asset_key

    def get_deps_asset_key(self, stream_definition) -> Iterable[AssetKey]:
        return self.deps


class EgressReplicationSpec(Resolvable, Model):
    name: str
    deps: Sequence[ResolvedAssetKey]
    asset_key_prefix: Optional[str] = None


class EgressReplicationComponent(Component, Resolvable, Model):
    """Component for building @sling_assets for non-sharded (egress) replications."""

    config_dir: Annotated[Path, Resolver(_resolve_path, model_field_type=str)]
    replications: Sequence[EgressReplicationSpec]

    @classmethod
    def get_additional_scope(cls) -> Mapping[str, Any]:
        return {"dbt_asset_key": dbt_asset_key}

    def get_config_path(self, config_file: str) -> Path:
        return Path(self.config_dir).joinpath(config_file)

    def build_defs(self, context: ComponentLoadContext):
        assets = []
        for replication in self.replications:
            cfg_path = self.config_dir.joinpath(f"{replication.name}.yaml")
            check.invariant(
                cfg_path.exists(),
                f"For replication named {replication.name}, expected file {cfg_path} does not exist.",
            )
            main_assets, _ = build_sling_assets(
                name=replication.name,
                config_path=cfg_path,
                build_deps=False,
                group_name="sling_egress",
                translator=EgressReplicationSlingTranslator(
                    asset_key_prefix=replication.asset_key_prefix,
                    deps=replication.deps,
                ),
            )
            assets.append(main_assets)
        return Definitions(assets=assets)
