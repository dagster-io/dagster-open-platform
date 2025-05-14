from collections.abc import Iterable, Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, Optional

import dagster_shared.check as check
from dagster import (
    AssetKey,
    AutoMaterializePolicy,
    AutomationCondition,
    Definitions,
    SourceAsset,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
)
from dagster.components import (
    Component,
    ComponentLoadContext,
    Model,
    ResolutionContext,
    Resolvable,
    Resolver,
)
from dagster_sling import DagsterSlingTranslator, SlingResource, sling_assets


class CustomSlingTranslator(DagsterSlingTranslator):
    def __init__(self, cron_schedule: str = "*/5 * * * *", shard_name: str = "main"):
        super().__init__()
        self.cron_schedule = cron_schedule
        self.shard_name = shard_name

    def get_tags(self, stream_definition: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"dagster/kind/snowflake": ""}

    def get_metadata(self, stream_definition: Mapping[str, Any]) -> Mapping[str, Any]:
        key: AssetKey = self.get_asset_key(stream_definition)
        return {
            **super().get_metadata(stream_definition),
            "dagster/table_name": ".".join(key.path),
        }

    def get_auto_materialize_policy(
        self, stream_definition: Mapping[str, Any]
    ) -> AutoMaterializePolicy | None:
        return (
            AutomationCondition.cron_tick_passed(self.cron_schedule)
            & ~AutomationCondition.in_progress()
        ).as_auto_materialize_policy()

    def get_group_name(self, stream_definition):
        return f"cloud_product_{self.shard_name}"

    def get_deps_asset_key(self, stream_definition) -> Iterable[AssetKey]:
        stream_asset_key = next(iter(super().get_deps_asset_key(stream_definition)))
        return [AssetKey([self.shard_name, *stream_asset_key.path])]


class DopReplicationSpec(Resolvable, Model):
    name: str
    shards: Sequence[str]
    cron_schedule: str = "*/5 * * * *"
    last_update_freshness_check: Optional[Mapping[str, int]] = None


def _resolve_path(context: ResolutionContext, val: str):
    p = context.resolve_source_relative_path(val)
    check.invariant(p.exists(), f"resolved config_dir Path {p} does not exist")
    return p


class ProdDbReplicationsComponent(Component, Resolvable, Model):
    """Custom component for building @sling_assets for replications against the sharded product DBs.

    For a replication spec named "foo" we will, for each shard in shards:
    * expect `shard_foo.yaml` to exist in the config_dir
    * use those to build @sling_assets named `cloud_product_shard_foo` (when shard name is "main" it is omitted)
    * by default schedule it to run every 5 minutes, but can be overridden with cron_schedule
    * optionally create a last update freshness check with provided timedelta arguments if set
    """

    config_dir: Annotated[Path, Resolver(_resolve_path, model_field_type=str)]
    replications: Sequence[DopReplicationSpec]

    def get_config_path(self, config_file: str) -> Path:
        return Path(self.config_dir).joinpath(config_file)

    def build_sling_assets(
        self,
        name: str,
        config_path: Path,
        shard_name: str,
        cron_schedule: str,
        last_update_freshness_check: Optional[Mapping[str, int]],
    ):
        @sling_assets(
            name=name,
            replication_config=str(config_path),
            dagster_sling_translator=CustomSlingTranslator(
                cron_schedule=cron_schedule,
                shard_name=shard_name,
            ),
        )
        def _dop_sling_assets(context, embedded_elt: SlingResource) -> Iterable[Any]:
            yield from (
                embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()
            )

        assets = [
            _dop_sling_assets,
            *(
                SourceAsset(key, group_name=f"postgres_{shard_name}")
                for key in _dop_sling_assets.dependency_keys
            ),
        ]
        checks = []
        if last_update_freshness_check:
            checks = build_last_update_freshness_checks(
                assets=[_dop_sling_assets],
                lower_bound_delta=timedelta(**last_update_freshness_check),
            )

        return assets, checks

    def build_defs(self, context: ComponentLoadContext):
        assets = []
        checks = []

        for replication in self.replications:
            for shard in replication.shards:
                name = (
                    f"cloud_product_{replication.name}"
                    if shard == "main"
                    else f"cloud_product_{shard}_{replication.name}"
                )
                cfg_path = self.config_dir.joinpath(f"{shard}_{replication.name}.yaml")
                check.invariant(
                    cfg_path.exists(),
                    f"For replication named {replication.name}, expected file {cfg_path} does not exist.",
                )

                main_assets, main_checks = self.build_sling_assets(
                    name=name,
                    config_path=cfg_path,
                    shard_name=shard,
                    cron_schedule=replication.cron_schedule,
                    last_update_freshness_check=replication.last_update_freshness_check,
                )
                assets.extend(main_assets)
                checks.extend(main_checks)

        return Definitions(
            assets=assets,
            asset_checks=checks,
            sensors=[
                build_sensor_for_freshness_checks(freshness_checks=checks),
            ],
        )
