import os

from dagster import MaterializeResult, asset
from dagster_dbt import get_asset_key_for_model

from ..resources import ConfigurableHightouchResource
from .dbt import dbt_non_partitioned_models

org_activity_monthly = get_asset_key_for_model([dbt_non_partitioned_models], "org_activity_monthly")


@asset(deps=[org_activity_monthly], compute_kind="hightouch", group_name="sales")
def hightouch_org_activity_monthly(hightouch: ConfigurableHightouchResource) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_ORG_ACTIVITY_MONTHLY_SYNC_ID", ""))
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


org_info = get_asset_key_for_model([dbt_non_partitioned_models], "org_info")


@asset(deps=[org_info], compute_kind="hightouch", group_name="sales")
def hightouch_org_info(hightouch: ConfigurableHightouchResource) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_ORG_INFO_SYNC_ID", ""))
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


@asset(
    deps=[
        get_asset_key_for_model([dbt_non_partitioned_models], "salesforce_contacts"),
        get_asset_key_for_model([dbt_non_partitioned_models], "stg_cloud_product__users"),
    ],
    compute_kind="hightouch",
    group_name="sales",
)
def hightouch_null_contact_names(hightouch: ConfigurableHightouchResource) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_CONTACT_NAMES_SYNC_ID", ""))
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


cloud_users = get_asset_key_for_model([dbt_non_partitioned_models], "cloud_users")


@asset(
    deps=[cloud_users],
    compute_kind="hightouch",
    group_name="sales",
)
def hightouch_cloud_users(hightouch: ConfigurableHightouchResource) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_CLOUD_USERS_SYNC_ID", ""))
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
