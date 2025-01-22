from pathlib import Path
from typing import Any, Iterable, Mapping

from dagster import AssetKey, AutoMaterializePolicy, AutomationCondition, SourceAsset
from dagster_open_platform.utils.environment_helpers import (
    get_environment,
    get_schema_for_environment,
)
from dagster_sling import DagsterSlingTranslator, SlingResource, sling_assets

cloud_product_config_dir = Path(__file__).parent / "configs" / "cloud_product"


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


@sling_assets(
    replication_config=cloud_product_config_dir / "main_low_volume.yaml",
    dagster_sling_translator=CustomSlingTranslator(cron_schedule="0 */2 * * *"),
)
def cloud_product_main_low_volume(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()


@sling_assets(
    replication_config=cloud_product_config_dir / "main_high_volume.yaml",
    dagster_sling_translator=CustomSlingTranslator(),
)
def cloud_product_main_high_volume(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()


@sling_assets(
    replication_config=cloud_product_config_dir / "main_event_log.yaml",
    dagster_sling_translator=CustomSlingTranslator(),
)
def cloud_product_main_event_log(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()


@sling_assets(
    replication_config=cloud_product_config_dir / "main_runs.yaml",
    dagster_sling_translator=CustomSlingTranslator(),
)
def cloud_product_main_runs(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()


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


@sling_assets(
    replication_config=cloud_product_config_dir / "shard1_low_volume.yaml",
    dagster_sling_translator=CustomSlingTranslator(
        cron_schedule="0 */2 * * *", shard_name="shard1"
    ),
)
def cloud_product_shard1_low_volume(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()


@sling_assets(
    replication_config=cloud_product_config_dir / "shard1_high_volume.yaml",
    dagster_sling_translator=CustomSlingTranslator(shard_name="shard1"),
)
def cloud_product_shard1_high_volume(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()


@sling_assets(
    replication_config=cloud_product_config_dir / "shard1_event_log.yaml",
    dagster_sling_translator=CustomSlingTranslator(shard_name="shard1"),
)
def cloud_product_shard1_event_log(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()


@sling_assets(
    replication_config=cloud_product_config_dir / "shard1_runs.yaml",
    dagster_sling_translator=CustomSlingTranslator(shard_name="shard1"),
)
def cloud_product_shard1_runs(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()


@sling_assets(
    replication_config=cloud_product_config_dir / "main_full_refresh.yaml",
    dagster_sling_translator=CustomSlingTranslator(cron_schedule="0 */2 * * *"),
)
def cloud_product_full_refresh(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()


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


class CustomSlingTranslatorEgress(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "sling_egress"

    def get_deps_asset_key(self, stream_definition) -> Iterable[AssetKey]:
        stream_asset_key = next(iter(super().get_deps_asset_key(stream_definition)))
        db, schema, table = stream_asset_key.path
        db = "sandbox" if get_environment() == "LOCAL" else "purina"
        schema = get_schema_for_environment(schema)
        return [AssetKey([db, schema, table])]


@sling_assets(
    replication_config=reporting_db_config_dir / "salesforce_contract_info.yaml",
    dagster_sling_translator=CustomSlingTranslatorEgress(),
)
def salesforce_contract_info(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count()
