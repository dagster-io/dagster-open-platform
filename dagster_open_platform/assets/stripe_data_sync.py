"""Source assets representing stripe's data sync process."""

from typing import Iterator

from dagster import (
    AssetKey,
    AssetSpec,
    FreshnessPolicy,
    MetadataValue,
    ObserveResult,
    ScheduleDefinition,
    define_asset_job,
    multi_observable_source_asset,
)
from dagster._core.definitions.asset_selection import AssetSelection
from dagster._core.definitions.schedule_definition import DefaultScheduleStatus
from dagster._core.execution.context.compute import AssetExecutionContext
from dagster_snowflake import SnowflakeResource
from dagster_snowflake.resources import fetch_last_updated_timestamps

STRIPE_SYNC_DATABASE = "STRIPE_PIPELINE"
STRIPE_SYNC_SCHEMA = "STRIPE"

# Read stripe's documentation to understand sync freshness guarantees.
# https://docs.stripe.com/stripe-data/available-data
stripe_sync_freshness_policy = FreshnessPolicy(
    maximum_lag_minutes=9 * 60,
)

table_names = [
    "BALANCE_TRANSACTIONS",
    "CHARGES",
    "COUPONS",
    "CUSTOMERS",
    "INVOICE_LINE_ITEMS",
    "INVOICES",
    "PLANS",
    "SUBSCRIPTION_ITEMS",
    "SUBSCRIPTION_SCHEDULE_PHASES",
    "SUBSCRIPTION_SCHEDULES",
    "SUBSCRIPTIONS",
    "SUBSCRIPTIONS_METADATA",
]

table_names_to_asset_keys = {
    table_name: AssetKey(
        [STRIPE_SYNC_DATABASE.lower(), STRIPE_SYNC_SCHEMA.lower(), table_name.lower()]
    )
    for table_name in table_names
}
asset_keys_to_table_names = {v: k for k, v in table_names_to_asset_keys.items()}

asset_specs = [
    AssetSpec(
        key=table_names_to_asset_keys[table_name],
        description=f"Stripe {table_name} table (synced using stripe pipeline)",
        freshness_policy=stripe_sync_freshness_policy,
    )
    for table_name in table_names
]


@multi_observable_source_asset(specs=asset_specs, can_subset=True)
def stripe_data_sync_assets(
    context: AssetExecutionContext, snowflake: SnowflakeResource
) -> Iterator[ObserveResult]:
    """Assets representing stripe's data sync process."""
    subsetted_table_names = [
        asset_keys_to_table_names[asset_key] for asset_key in context.selected_asset_keys
    ]
    with snowflake.get_connection() as conn:
        freshness_results = fetch_last_updated_timestamps(
            snowflake_connection=conn,
            schema=STRIPE_SYNC_SCHEMA,
            tables=subsetted_table_names,
            database=STRIPE_SYNC_DATABASE,
        )
        for table_name, last_updated in freshness_results.items():
            yield ObserveResult(
                asset_key=table_names_to_asset_keys[table_name],
                metadata={
                    "dagster/last_updated_timestamp": MetadataValue.timestamp(last_updated),
                },
            )


stripe_data_sync_schedule = ScheduleDefinition(
    cron_schedule="0 0 * * *",
    job=define_asset_job(
        name="stripe_data_sync_observe_job",
        selection=AssetSelection.keys(*asset_keys_to_table_names.keys()),
    ),
    default_status=DefaultScheduleStatus.RUNNING,
)
