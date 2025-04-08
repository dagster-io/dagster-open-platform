from dagster import Definitions, load_assets_from_modules
from dagster_open_platform.defs.dbt import assets
from dagster_open_platform.defs.dbt.resources import dbt_resource
from dagster_open_platform.defs.dbt.schedules import scheduled_jobs, schedules
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

dbt_assets = load_assets_from_modules([assets])


defs = Definitions(
    assets=add_code_references_and_link_to_git(dbt_assets),
    asset_checks=assets.snapshots_freshness_checks,
    resources={
        "dbt": dbt_resource,
    },
    jobs=scheduled_jobs,
    schedules=schedules,
)
