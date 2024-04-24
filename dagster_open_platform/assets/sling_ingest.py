from pathlib import Path

from dagster_embedded_elt.sling.asset_decorator import sling_assets
from dagster_embedded_elt.sling.dagster_sling_translator import DagsterSlingTranslator
from dagster_embedded_elt.sling.resources import (
    SlingResource,
)

config_dir = Path(__file__).parent.parent / "configs" / "sling" / "cloud_product"


class CustomSlingLowVolumeTranslator(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "cloud_product_low_volume_ingest"


@sling_assets(
    replication_config=config_dir / "main_low_volume.yaml",
    dagster_sling_translator=CustomSlingLowVolumeTranslator(),
)
def cloud_product_main_low_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context)


@sling_assets(
    replication_config=config_dir / "shard1_low_volume.yaml",
    dagster_sling_translator=CustomSlingLowVolumeTranslator(),
)
def cloud_product_shard1_low_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context)


class CustomSlingHighVolumeTranslator(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "cloud_product_high_volume_ingest"


@sling_assets(
    replication_config=config_dir / "main_high_volume.yaml",
    dagster_sling_translator=CustomSlingHighVolumeTranslator(),
)
def cloud_product_main_high_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context)


@sling_assets(
    replication_config=config_dir / "shard1_high_volume.yaml",
    dagster_sling_translator=CustomSlingHighVolumeTranslator(),
)
def cloud_product_shard1_high_volume(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context)
