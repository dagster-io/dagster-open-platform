import dagster as dg
from dagster.components import definitions
from dagster_snowflake import SnowflakeResource

# All tables synced by the EU sling ingestion in dagster_open_platform_eu.
# Each entry maps to an upstream EU sling asset at eu/sling/cloud_product/<table>
# and a downstream replicated asset at sling/cloud_product_eu/<table> in US Snowflake.
_EU_TABLES = [
    # xregion shard (low_volume)
    "organizations",
    # main shard (low_volume)
    "customer_info",
    "deployments",
    "onboarding_checklist",
    "permissions",
    "serverless_agents",
    "session_tokens",
    "users",
    "users_organizations",
    "users_permissions",
    "teams",
    "teams_users",
    "teams_permissions",
    "jobs",
    "notifications",
    "alerts",
    "alerts_alert_policies_new",
    "catalog_views",
    "repository_locations_data",
    "enterprise_user_managed_expansions",
    # main shard (full_refresh)
    "users_organizations_current_state_ids",
    # main shard (high_volume)
    "run_tags",
    "alert_policies",
    # main shard (event_log)
    "event_logs",
    # main shard (user_event_log)
    "user_events_counts",
    # main shard (runs)
    "runs",
]


@dg.multi_asset(
    group_name="cloud_product_eu",
    can_subset=True,
    specs=[
        dg.AssetSpec(
            key=dg.AssetKey(["sling", "cloud_product_eu", table]),
            deps=[dg.AssetKey(["eu", "sling", "cloud_product", table])],
            automation_condition=dg.AutomationCondition.on_cron("0 * * * *"),
        )
        for table in _EU_TABLES
    ],
)
def eu_cloud_product_snowflake_replication(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
):
    """Refreshes the EU database replication and clones the EU cloud product schema
    into US Snowflake.
    """
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("ALTER DATABASE sling_eu REFRESH")
        cur.execute("CREATE OR REPLACE SCHEMA SLING.CLOUD_PRODUCT_EU CLONE SLING_EU.CLOUD_PRODUCT")

    for key in context.selected_asset_keys:
        yield dg.MaterializeResult(asset_key=key)


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(assets=[eu_cloud_product_snowflake_replication])
