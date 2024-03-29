import os

from dagster_embedded_elt.sling.asset_decorator import sling_assets
from dagster_embedded_elt.sling.dagster_sling_translator import DagsterSlingTranslator
from dagster_embedded_elt.sling.resources import (
    SlingResource,
)

config_dir = "dagster_open_platform/configs/sling/cloud_product"


class CustomSlingLowVolumeTranslator(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "cloud_product_low_volume_ingest"


cloud_production_low_volume_config = os.path.join(config_dir, "low_volume.yaml")


@sling_assets(
    replication_config=cloud_production_low_volume_config,
    dagster_sling_translator=CustomSlingLowVolumeTranslator(),
)
def cloud_product_low_volume(embedded_elt: SlingResource):
    yield from embedded_elt.replicate(  # type: ignore
        replication_config=cloud_production_low_volume_config,
        dagster_sling_translator=CustomSlingLowVolumeTranslator(),
    )


class CustomSlingHighVolumeTranslator(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "cloud_product_high_volume_ingest"


cloud_production_high_volume_config = os.path.join(config_dir, "high_volume.yaml")


@sling_assets(
    replication_config=cloud_production_high_volume_config,
    dagster_sling_translator=CustomSlingHighVolumeTranslator(),
)
def cloud_product_high_volume(embedded_elt: SlingResource):
    yield from embedded_elt.replicate(  # type: ignore
        replication_config=cloud_production_high_volume_config,
        dagster_sling_translator=CustomSlingHighVolumeTranslator(),
    )
