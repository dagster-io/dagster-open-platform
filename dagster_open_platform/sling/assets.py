from pathlib import Path
from typing import Any, Mapping

from dagster import AssetKey, SourceAsset
from dagster_embedded_elt.sling.asset_decorator import sling_assets
from dagster_embedded_elt.sling.dagster_sling_translator import DagsterSlingTranslator
from dagster_embedded_elt.sling.resources import SlingResource
from dagster_open_platform.utils.environment_helpers import (
    get_environment,
    get_schema_for_environment,
)

cloud_product_config_dir = Path(__file__).parent / "configs" / "cloud_product"


class CustomSlingTranslatorBase(DagsterSlingTranslator):
    def get_tags(self, stream_definition: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"dagster/storage_kind": "snowflake"}

    def get_metadata(self, stream_definition: Mapping[str, Any]) -> Mapping[str, Any]:
        key: AssetKey = self.get_asset_key(stream_definition)
        return {
            **super().get_metadata(stream_definition),
            "dagster/relation_identifier": ".".join(key.path),
        }


class CustomSlingTranslatorMain(CustomSlingTranslatorBase):
    def get_group_name(self, stream_definition):
        return "cloud_product_main"

    def get_deps_asset_key(self, stream_definition):
        stream_asset_key = super().get_deps_asset_key(stream_definition)[0]
        return AssetKey(["main", *stream_asset_key[0]])


@sling_assets(
    replication_config=cloud_product_config_dir / "main_low_volume.yaml",
    dagster_sling_translator=CustomSlingTranslatorMain(),
)
def cloud_product_main_low_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context).fetch_column_metadata()


@sling_assets(
    replication_config=cloud_product_config_dir / "main_high_volume.yaml",
    dagster_sling_translator=CustomSlingTranslatorMain(),
)
def cloud_product_main_high_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context).fetch_column_metadata()


@sling_assets(
    replication_config=cloud_product_config_dir / "main_event_log.yaml",
    dagster_sling_translator=CustomSlingTranslatorMain(),
)
def cloud_product_main_event_log(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context).fetch_column_metadata()


@sling_assets(
    replication_config=cloud_product_config_dir / "main_runs.yaml",
    dagster_sling_translator=CustomSlingTranslatorMain(),
)
def cloud_product_main_runs(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context).fetch_column_metadata()


cloud_product_main_source_assets = [
    *[
        SourceAsset(key, group_name="postgres_main")
        for key in cloud_product_main_low_volume.dependency_keys
    ],
    *[
        SourceAsset(key, group_name="postgres_main")
        for key in cloud_product_main_high_volume.dependency_keys
    ],
    *[
        SourceAsset(key, group_name="postgres_main")
        for key in cloud_product_main_event_log.dependency_keys
    ],
    *[
        SourceAsset(key, group_name="postgres_main")
        for key in cloud_product_main_runs.dependency_keys
    ],
]


class CustomSlingTranslatorShard1(CustomSlingTranslatorBase):
    def get_group_name(self, stream_definition):
        return "cloud_product_shard1"

    def get_deps_asset_key(self, stream_definition):
        stream_asset_key = super().get_deps_asset_key(stream_definition)[0]
        return AssetKey(["shard1", *stream_asset_key[0]])


@sling_assets(
    replication_config=cloud_product_config_dir / "shard1_low_volume.yaml",
    dagster_sling_translator=CustomSlingTranslatorShard1(),
)
def cloud_product_shard1_low_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context).fetch_column_metadata()


@sling_assets(
    replication_config=cloud_product_config_dir / "shard1_high_volume.yaml",
    dagster_sling_translator=CustomSlingTranslatorShard1(),
)
def cloud_product_shard1_high_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context).fetch_column_metadata()


@sling_assets(
    replication_config=cloud_product_config_dir / "shard1_event_log.yaml",
    dagster_sling_translator=CustomSlingTranslatorShard1(),
)
def cloud_product_shard1_event_log(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context).fetch_column_metadata()


@sling_assets(
    replication_config=cloud_product_config_dir / "shard1_runs.yaml",
    dagster_sling_translator=CustomSlingTranslatorShard1(),
)
def cloud_product_shard1_runs(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context).fetch_column_metadata()


cloud_product_shard1_source_assets = [
    *[
        SourceAsset(key, group_name="postgres_shard1")
        for key in cloud_product_shard1_low_volume.dependency_keys
    ],
    *[
        SourceAsset(key, group_name="postgres_shard1")
        for key in cloud_product_shard1_high_volume.dependency_keys
    ],
    *[
        SourceAsset(key, group_name="postgres_shard1")
        for key in cloud_product_shard1_event_log.dependency_keys
    ],
    *[
        SourceAsset(key, group_name="postgres_shard1")
        for key in cloud_product_shard1_runs.dependency_keys
    ],
]


reporting_db_config_dir = Path(__file__).parent / "configs" / "reporting_db"


class CustomSlingTranslator(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "sling_egress"

    def get_deps_asset_key(self, stream_definition):
        stream_asset_key = super().get_deps_asset_key(stream_definition)[0]
        db, schema, table = stream_asset_key[0]
        db = "sandbox" if get_environment() == "LOCAL" else "purina"
        schema = get_schema_for_environment(schema)
        return AssetKey([db, schema, table])


@sling_assets(
    replication_config=reporting_db_config_dir / "salesforce_contract_info.yaml",
    dagster_sling_translator=CustomSlingTranslator(),
)
def salesforce_contract_info(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context).fetch_column_metadata()
