from dagster import Definitions, load_assets_from_modules
from dagster_open_platform.sling import assets
from dagster_open_platform.sling.resources import embedded_elt_resource
from dagster_open_platform.sling.schedules import (
    cloud_product_sync_high_volume_schedule,
    cloud_product_sync_low_volume_schedule,
    sling_egress_schedule,
)

from ..utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git(load_assets_from_modules([assets])),
    resources={
        "embedded_elt": embedded_elt_resource,
    },
    schedules=[
        cloud_product_sync_high_volume_schedule,
        cloud_product_sync_low_volume_schedule,
        sling_egress_schedule,
    ],
    # asset_checks=[*freshness_checks],
    # sensors=[freshness_checks_sensor],
)
