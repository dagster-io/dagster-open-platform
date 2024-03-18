"""Source assets representing stripe's data sync process."""

from typing import Iterator

from dagster import (
    AssetKey,
    AssetSpec,
    FreshnessPolicy,
    MetadataValue,
    ObserveResult,
    multi_observable_source_asset,
)
from dagster_snowflake import SnowflakeResource
from dagster_snowflake.resources import fetch_last_updated_timestamps

STRIPE_SYNC_DATABASE = "stripe_pipeline"
STRIPE_SYNC_SCHEMA = "stripe"

# Read stripe's documentation to understand sync freshness guarantees.
# https://docs.stripe.com/stripe-data/available-data
stripe_sync_freshness_policy = FreshnessPolicy(
    maximum_lag_minutes=9 * 60,
)

table_names = [
    "charges",
    "customers",
    "disputes",
    "events",
    "invoices",
    "invoice_items",
    "payments",
    "payouts",
    "refunds",
    "subscriptions",
    "subscription_items",
    "tax_rates",
]
table_names_to_asset_keys = {
    table_name: AssetKey([STRIPE_SYNC_DATABASE, STRIPE_SYNC_SCHEMA, table_name])
    for table_name in table_names
}

asset_specs = [
    AssetSpec(
        key=table_names_to_asset_keys[table_name],
        description=f"Stripe {table_name} table (synced using stripe pipeline)",
        freshness_policy=stripe_sync_freshness_policy,
    )
    for table_name in table_names
]


@multi_observable_source_asset(specs=asset_specs)
def stripe_data_sync_assets(context, snowflake: SnowflakeResource) -> Iterator[ObserveResult]:
    """Assets representing stripe's data sync process."""
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"USE DATABASE {STRIPE_SYNC_DATABASE}")

        freshness_results = fetch_last_updated_timestamps(
            snowflake_connection=conn,
            schema=STRIPE_SYNC_SCHEMA,
            tables=table_names,
        )
        for table_name, last_updated in freshness_results.items():
            yield ObserveResult(
                asset_key=table_names_to_asset_keys[table_name],
                metadata={
                    "dagster/last_updated_timestamp": MetadataValue.timestamp(last_updated),
                },
            )
