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
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
)
from dagster.components import Component, ComponentLoadContext, Model, Resolvable, Resolver
from dagster_open_platform.lib.sling.utils import _resolve_path, build_sling_assets
from dagster_sling import DagsterSlingTranslator


class ProdDbReplicationsSlingTranslator(DagsterSlingTranslator):
    def __init__(
        self,
        cron_schedule: str = "*/5 * * * *",
        shard_name: str = "main",
    ):
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

    def get_group_name(self, stream_definition):
        return f"cloud_product_{self.shard_name}"

    def get_deps_asset_key(self, stream_definition) -> Iterable[AssetKey]:
        stream_asset_key = next(iter(super().get_deps_asset_key(stream_definition)))
        return [AssetKey([self.shard_name, *stream_asset_key.path])]

    def get_auto_materialize_policy(
        self, stream_definition: Mapping[str, Any]
    ) -> AutoMaterializePolicy | None:
        return (
            AutomationCondition.cron_tick_passed(self.cron_schedule)
            & ~AutomationCondition.in_progress()
        ).as_auto_materialize_policy()


class DopReplicationSpec(Resolvable, Model):
    name: str
    shards: Sequence[str]
    cron_schedule: str = "*/5 * * * *"
    last_update_freshness_check: Optional[Mapping[str, int]] = None


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

                main_assets, deps = build_sling_assets(
                    name=name,
                    config_path=cfg_path,
                    group_name=f"postgres_{shard}",
                    translator=ProdDbReplicationsSlingTranslator(
                        shard_name=shard,
                        cron_schedule=replication.cron_schedule,
                    ),
                )

                assets.append(main_assets)
                assets.extend(deps)

                if replication.last_update_freshness_check:
                    main_checks = build_last_update_freshness_checks(
                        assets=[main_assets],
                        lower_bound_delta=timedelta(**replication.last_update_freshness_check),
                    )
                    checks.extend(main_checks)
        return Definitions(
            assets=assets,
            asset_checks=checks,
            sensors=[
                build_sensor_for_freshness_checks(freshness_checks=checks),
            ]
            if checks
            else [],
        )
