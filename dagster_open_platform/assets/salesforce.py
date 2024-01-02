import os

from dagster import MaterializeResult, asset
from dagster_dbt import get_asset_key_for_model

from ..resources import ConfigurableHightouchResource
from .dbt import cloud_analytics_dbt_assets

sf_usage_metrics_daily = get_asset_key_for_model(
    [cloud_analytics_dbt_assets], "sf_export_usage_metrics_daily"
)


@asset(deps=[sf_usage_metrics_daily], compute_kind="hightouch", group_name="sales")
def hightouch_usage_metrics_daily(hightouch: ConfigurableHightouchResource) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_USAGE_METRICS_DAILY_SYNC_ID", ""))
    return MaterializeResult(
        metadata={
            "sync_details": result.sync_details,
            "sync_run_details": result.sync_run_details,
            "destination_details": result.destination_details,
            "query_size": result.sync_run_details.get("querySize"),
            "completion_ratio": result.sync_run_details.get("completionRatio"),
            "failed_rows": result.sync_run_details.get("failedRows", {}).get("addedCount"),
        }
    )
