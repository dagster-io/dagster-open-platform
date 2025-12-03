"""Source assets representing stripe's data sync process."""

import datetime
from collections.abc import Iterator

from dagster import (
    AssetKey,
    AssetSpec,
    Definitions,
    MetadataValue,
    ObserveResult,
    ScheduleDefinition,
    build_last_update_freshness_checks,
    define_asset_job,
    multi_observable_source_asset,
)
from dagster._core.definitions.asset_selection import AssetSelection
from dagster._core.definitions.schedule_definition import DefaultScheduleStatus
from dagster._core.execution.context.compute import AssetExecutionContext
from dagster.components import definitions
from dagster_snowflake import SnowflakeResource
from dagster_snowflake.resources import fetch_last_updated_timestamps

STRIPE_SYNC_DATABASE = "STRIPE_PIPELINE"
STRIPE_SYNC_SCHEMA = "STRIPE"

# Read stripe's documentation to understand sync freshness guarantees.
# https://docs.stripe.com/stripe-data/available-data
maximum_time_between_syncs = datetime.timedelta(hours=9)

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

stripe_pipeline_freshness_checks = build_last_update_freshness_checks(
    assets=list(table_names_to_asset_keys.values()),
    lower_bound_delta=maximum_time_between_syncs,
)
asset_specs = [
    AssetSpec(
        key=table_names_to_asset_keys[table_name],
        description=f"Stripe {table_name} table (synced using stripe pipeline)",
    )
    for table_name in table_names
]


@multi_observable_source_asset(specs=asset_specs, can_subset=True, group_name="stripe_pipeline")
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


@definitions
def defs():
    stripe_data_sync_schedule = ScheduleDefinition(
        cron_schedule="0 0 * * *",
        job=define_asset_job(
            name="stripe_data_sync_observe_job",
            selection=AssetSelection.keys(*asset_keys_to_table_names.keys()),
        ),
        default_status=DefaultScheduleStatus.RUNNING,
    )

    return Definitions(
        assets=[stripe_data_sync_assets],
        schedules=[stripe_data_sync_schedule],
        resources={},
    )
