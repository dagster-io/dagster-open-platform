from datetime import timedelta
from pathlib import Path

from dagster import (
    AssetKey,
    AssetsDefinition,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    SourceAsset,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
)
from dagster_embedded_elt.sling.asset_decorator import sling_assets
from dagster_embedded_elt.sling.dagster_sling_translator import DagsterSlingTranslator
from dagster_embedded_elt.sling.resources import (
    SlingResource,
)

config_dir = Path(__file__).parent.parent / "configs" / "sling" / "cloud_product"


class CustomSlingTranslatorMain(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "cloud_product_main"

    def get_deps_asset_key(self, stream_definition):
        stream_asset_key = super().get_deps_asset_key(stream_definition)[0]
        return AssetKey(["main", *stream_asset_key[0]])

    def get_auto_materialize_policy(self, stream_definition):
        high_volume_asset_keys = [
            "event_logs",
            "runs",
            "run_tags",
            "asset_materializations",
            "asset_observations",
            "asset_partitions",
            "alert_policies",
        ]
        stream_asset_key = super().get_deps_asset_key(stream_definition)[0][0][-1]

        if stream_asset_key in high_volume_asset_keys:
            return AutoMaterializePolicy.eager().with_rules(
                AutoMaterializeRule.materialize_on_cron("*/5 * * * *"),
            )
        return AutoMaterializePolicy.eager().with_rules(
            AutoMaterializeRule.materialize_on_cron("0 */2 * * *"),
        )


@sling_assets(
    replication_config=config_dir / "main_low_volume.yaml",
    dagster_sling_translator=CustomSlingTranslatorMain(),
)
def cloud_product_main_low_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context)


@sling_assets(
    replication_config=config_dir / "main_high_volume.yaml",
    dagster_sling_translator=CustomSlingTranslatorMain(),
)
def cloud_product_main_high_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context)


cloud_product_main_source_assets = [
    *[
        SourceAsset(key, group_name="postgres_main")
        for key in cloud_product_main_low_volume.dependency_keys
    ],
    *[
        SourceAsset(key, group_name="postgres_main")
        for key in cloud_product_main_high_volume.dependency_keys
    ],
]


class CustomSlingTranslatorShard1(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "cloud_product_shard1"

    def get_deps_asset_key(self, stream_definition):
        stream_asset_key = super().get_deps_asset_key(stream_definition)[0]
        return AssetKey(["shard1", *stream_asset_key[0]])

    def get_auto_materialize_policy(self, stream_definition):
        high_volume_asset_keys = [
            "event_logs",
            "runs",
            "run_tags",
            "asset_materializations",
            "asset_observations",
            "asset_partitions",
            "alert_policies",
        ]
        stream_asset_key = super().get_deps_asset_key(stream_definition)[0][0][-1]

        if stream_asset_key in high_volume_asset_keys:
            return AutoMaterializePolicy.eager().with_rules(
                AutoMaterializeRule.materialize_on_cron("*/5 * * * *"),
            )
        return AutoMaterializePolicy.eager().with_rules(
            AutoMaterializeRule.materialize_on_cron("0 */2 * * *"),
        )


@sling_assets(
    replication_config=config_dir / "shard1_low_volume.yaml",
    dagster_sling_translator=CustomSlingTranslatorShard1(),
)
def cloud_product_shard1_low_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context)


@sling_assets(
    replication_config=config_dir / "shard1_high_volume.yaml",
    dagster_sling_translator=CustomSlingTranslatorShard1(),
)
def cloud_product_shard1_high_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context)


cloud_product_shard1_source_assets = [
    *[
        SourceAsset(key, group_name="postgres_shard1")
        for key in cloud_product_shard1_low_volume.dependency_keys
    ],
    *[
        SourceAsset(key, group_name="postgres_shard1")
        for key in cloud_product_shard1_high_volume.dependency_keys
    ],
]


freshness_checks = build_last_update_freshness_checks(
    assets=[
        AssetKey(["sling", "cloud_product", "event_logs"]),
        AssetKey(["sling", "cloud_product_shard1", "event_logs"]),
    ],
    lower_bound_delta=timedelta(minutes=15),
)
# NOTE: this instance check is present while we're switching from returning an AssetsDefinition to
# a sequence of AssetDefinition objects. It can be removed once the lastest release contains this
# change.
event_logs_freshness_checks = (
    [freshness_checks] if isinstance(freshness_checks, AssetsDefinition) else freshness_checks
)

freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=event_logs_freshness_checks  # type: ignore
)
