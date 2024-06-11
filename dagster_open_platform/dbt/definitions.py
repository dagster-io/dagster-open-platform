from dagster import Definitions, load_assets_from_modules
from dagster_open_platform.dbt import assets
from dagster_open_platform.dbt.resources import dbt_resource
from dagster_open_platform.dbt.schedules import scheduled_jobs, schedules

dbt_assets = load_assets_from_modules([assets])


defs = Definitions(
    assets=dbt_assets,
    # asset_checks=assets.usage_metrics_daily_freshness_checks,
    resources={
        "dbt": dbt_resource,
    },
    jobs=scheduled_jobs,
    schedules=schedules,
)
