import os

from dagster import MetadataValue, RetryPolicy

from ..resources.stitch_resource import build_stitch_assets

SNOWFLAKE_ACCOUNT_BASE = os.getenv("SNOWFLAKE_ACCOUNT", ".").split(".")[0]
SNOWFLAKE_URL = (
    f"https://app.snowflake.com/ax61354/{SNOWFLAKE_ACCOUNT_BASE}/#/data/databases/STITCH/schemas"
)

SHARED_CONFIG = {
    "retry_policy": RetryPolicy(max_retries=2, delay=15),
    "table_to_metadata": lambda table: {
        "url": MetadataValue.url(f"{SNOWFLAKE_URL}/CLOUD_PROD_PUBLIC/table/{table.upper()}"),
    },
}

frequent_cloud_stitch_assets = build_stitch_assets(
    asset_key_prefix=["stitch", "cloud_prod_public"],
    source_id=os.getenv("STITCH_CLOUD_INCREMENTAL_SYNC_SOURCE_ID", ""),
    destination_tables=["event_logs", "run_tags", "runs"],
    op_tags={"dagster/concurrency_key": "frequent_cloud_stitch_assets"},
    group_name="cloud_incremental",
    **SHARED_CONFIG,
)

infrequent_cloud_stitch_assets = build_stitch_assets(
    asset_key_prefix=["stitch", "elementl_cloud_prod"],
    source_id=os.getenv("STITCH_CLOUD_FULL_SYNC_SOURCE_ID", ""),
    destination_tables=[
        "asset_keys",
        "customer_info",
        "deployments",
        "onboarding_checklist",
        "organizations",
        "permissions",
        "serverless_agents",
        "session_tokens",
        "users",
        "users_organizations",
        "users_permissions",
    ],
    op_tags={"dagster/concurrency_key": "infrequent_cloud_stitch_assets"},
    group_name="cloud_full_syncs",
    **SHARED_CONFIG,
)
