from dagster import Definitions, load_assets_from_modules

from .assets import health_check
from .resources.dbt_resource import dbt_resource
from .resources.stitch_resource import stitch_resource

all_assets = load_assets_from_modules([health_check])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": dbt_resource,
        "stitch": stitch_resource,
    },
)
