from dagster import Definitions, load_assets_from_modules

from .assets import health_check

all_assets = load_assets_from_modules([health_check])

defs = Definitions(
    assets=all_assets,
)
