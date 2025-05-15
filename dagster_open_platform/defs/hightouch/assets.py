import os

from dagster import Definitions, MaterializeResult, asset
from dagster.components import definitions
from dagster_dbt import get_asset_key_for_model
from dagster_open_platform.definitions import global_freshness_policy
from dagster_open_platform.defs.dbt.assets import dbt_non_partitioned_models
from dagster_open_platform.defs.hightouch.resources import ConfigurableHightouchResource
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

org_activity_monthly = get_asset_key_for_model([dbt_non_partitioned_models], "org_activity_monthly")


@asset(
    deps=[org_activity_monthly],
    tags={"dagster/kind/hightouch": "", "dagster/kind/salesforce": ""},
    group_name="hightouch_syncs",
    internal_freshness_policy=global_freshness_policy,
)
def hightouch_org_activity_monthly(
    hightouch: ConfigurableHightouchResource,
) -> MaterializeResult:
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


sync_salesforce_account = get_asset_key_for_model(
    [dbt_non_partitioned_models], "sync_salesforce_account"
)


@asset(
    deps=[sync_salesforce_account],
    tags={"dagster/kind/hightouch": "", "dagster/kind/salesforce": ""},
    group_name="hightouch_syncs",
    internal_freshness_policy=global_freshness_policy,
)
def hightouch_sync_salesforce_account(
    hightouch: ConfigurableHightouchResource,
) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_SYNC_SALESFORCE_ACCOUNT_SYNC_ID", ""))
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
    tags={"dagster/kind/hightouch": "", "dagster/kind/salesforce": ""},
    group_name="hightouch_syncs",
    internal_freshness_policy=global_freshness_policy,
)
def hightouch_null_contact_names(
    hightouch: ConfigurableHightouchResource,
) -> MaterializeResult:
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
    tags={"dagster/kind/hightouch": "", "dagster/kind/salesforce": ""},
    group_name="hightouch_syncs",
    internal_freshness_policy=global_freshness_policy,
)
def hightouch_cloud_users(
    hightouch: ConfigurableHightouchResource,
) -> MaterializeResult:
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


user_attribution = get_asset_key_for_model([dbt_non_partitioned_models], "user_attribution")


@asset(
    deps=[user_attribution],
    tags={"dagster/kind/hightouch": "", "dagster/kind/salesforce": ""},
    group_name="hightouch_syncs",
    internal_freshness_policy=global_freshness_policy,
)
def hightouch_user_attribution(
    hightouch: ConfigurableHightouchResource,
) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_USER_ATTRIBUTION_SYNC_ID", ""))
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


sales_cycles = get_asset_key_for_model([dbt_non_partitioned_models], "sales_cycles")


@asset(
    deps=[sales_cycles],
    tags={"dagster/kind/hightouch": "", "dagster/kind/salesforce": ""},
    group_name="hightouch_syncs",
    internal_freshness_policy=global_freshness_policy,
)
def hightouch_sales_cycles(
    hightouch: ConfigurableHightouchResource,
) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_SALES_CYCLES_SYNC_ID", ""))
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


sync_hubspot_company = get_asset_key_for_model([dbt_non_partitioned_models], "sync_hubspot_company")


@asset(
    deps=[sync_hubspot_company],
    tags={"dagster/kind/hightouch": "", "dagster/kind/salesforce": ""},
    group_name="hightouch_syncs",
    internal_freshness_policy=global_freshness_policy,
)
def hightouch_sync_hubspot_company(
    hightouch: ConfigurableHightouchResource,
) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_SYNC_HUBSPOT_COMPANY_SYNC_ID", ""))
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


sync_hubspot_contact = get_asset_key_for_model([dbt_non_partitioned_models], "sync_hubspot_contact")


@asset(
    deps=[sync_hubspot_contact],
    tags={"dagster/kind/hightouch": "", "dagster/kind/salesforce": ""},
    group_name="hightouch_syncs",
    internal_freshness_policy=global_freshness_policy,
)
def hightouch_sync_hubspot_contact(
    hightouch: ConfigurableHightouchResource,
) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_SYNC_HUBSPOT_CONTACT_SYNC_ID", ""))
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


sync_hubspot_organization = get_asset_key_for_model(
    [dbt_non_partitioned_models], "sync_hubspot_organization"
)


@asset(
    deps=[sync_hubspot_organization],
    tags={"dagster/kind/hightouch": "", "dagster/kind/salesforce": ""},
    group_name="hightouch_syncs",
    internal_freshness_policy=global_freshness_policy,
)
def hightouch_sync_hubspot_organization(
    hightouch: ConfigurableHightouchResource,
) -> MaterializeResult:
    result = hightouch.sync_and_poll(os.getenv("HIGHTOUCH_SYNC_HUBSPOT_ORGANIZATION_SYNC_ID", ""))
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


@definitions
def defs() -> Definitions:
    all_assets = [
        hightouch_cloud_users,
        hightouch_null_contact_names,
        hightouch_org_activity_monthly,
        hightouch_sales_cycles,
        hightouch_sync_hubspot_company,
        hightouch_sync_hubspot_contact,
        hightouch_sync_hubspot_organization,
        hightouch_sync_salesforce_account,
        hightouch_user_attribution,
    ]
    return Definitions(
        assets=add_code_references_and_link_to_git(all_assets),
    )
