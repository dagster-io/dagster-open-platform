from dagster import Definitions, load_assets_from_modules
from dagster_open_platform.defs.sling import assets
from dagster_open_platform.defs.sling.resources import embedded_elt_resource
from dagster_open_platform.defs.sling.schedules import sling_egress_schedule
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git(load_assets_from_modules([assets])),
    resources={
        "embedded_elt": embedded_elt_resource,
    },
    schedules=[
        sling_egress_schedule,
    ],
)
