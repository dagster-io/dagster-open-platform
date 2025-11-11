"""Datadog billing and usage data asset.

This asset fetches historical billing data from Datadog's usage/cost APIs and writes it to Snowflake.
"""

from datetime import date

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult
from dagster_open_platform.defs.datadog.py.resources import DatadogBillingResource
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource


def datadog_usage(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    datadog_billing: DatadogBillingResource,
) -> MaterializeResult:
    """Fetch Datadog historical billing data and write to Snowflake.

    This function fetches historical cost data from Datadog's billing APIs
    for the partition date range and writes the data to Snowflake.

    Args:
        context: Dagster execution context
        snowflake: Snowflake resource for database connection
        datadog_billing: Datadog billing resource for API authentication

    Returns:
        MaterializeResult with metadata about the operation
    """
    # Get partition date range
    partition_window = context.partition_time_window
    start_date = partition_window.start.date()
    end_date = partition_window.end.date()

    # Format dates as YYYY-MM-DD for API (first day of month for start, last day for end)
    start_month = start_date.strftime("%Y-%m-01")
    end_month = end_date.strftime("%Y-%m-%d")

    context.log.info(f"Fetching Datadog billing data from {start_month} to {end_month}")

    all_rows = []

    # Fetch historical cost data
    try:
        context.log.info("Fetching historical cost data...")
        historical_data = datadog_billing.get_historical_cost(start_month, end_month)
        context.log.info(f"Received historical cost data: {historical_data}")

        # Process historical cost data
        # Response structure: {"data": [{"id": "...", "attributes": {"org_name": "...", "public_id": "...", "charges": [...]}}]}
        if "data" in historical_data:
            for org_data in historical_data.get("data", []):
                org_id = org_data.get("id", "")
                org_attrs = org_data.get("attributes", {})
                org_name = org_attrs.get("org_name", "")
                org_public_id = org_attrs.get("public_id", org_id)
                account_name = org_attrs.get("account_name", "")
                account_public_id = org_attrs.get("account_public_id", "")
                region = org_attrs.get("region", "")
                total_cost = org_attrs.get("total_cost", 0)
                usage_date = org_attrs.get("date", "")
                charges = org_attrs.get("charges", [])

                # Handle both list of charge objects and single charge object
                if not isinstance(charges, list):
                    charges = [charges] if charges else []

                for charge in charges:
                    # Handle different charge record formats
                    if isinstance(charge, dict):
                        row = {
                            "org_public_id": org_public_id,
                            "org_name": org_name,
                            "account_name": account_name,
                            "account_public_id": account_public_id,
                            "region": region,
                            "total_cost": total_cost,
                            "date": usage_date,
                            "product_name": charge.get("product_name", ""),
                            "charge_type": charge.get("charge_type", ""),
                            "cost": charge.get("cost", 0),
                            "last_aggregation_function": (
                                str(charge.get("last_aggregation_function"))
                                if charge.get("last_aggregation_function") is not None
                                else None
                            ),
                            "recorded_at": str(context.run_id),
                            "partition_date": start_date.isoformat(),
                        }
                        all_rows.append(row)
    except Exception as e:
        context.log.warning(f"Failed to fetch historical cost data: {e}", exc_info=True)

    if not all_rows:
        context.log.warning("No billing data retrieved from Datadog APIs")
        return MaterializeResult(
            metadata={
                "rows_inserted": 0,
                "message": "No data retrieved from APIs",
            }
        )

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)
    context.log.info(f"Prepared {len(df)} rows for Snowflake")

    # Get database and schema names
    database_name = get_database_for_environment("DATADOG")
    schema_name = get_schema_for_environment("RAW")
    table_name = "USAGE"

    context.log.info(f"Writing to {database_name}.{schema_name}.{table_name}")

    # Write to Snowflake
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        # Create schema if not exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}")

        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
            org_public_id VARCHAR,
            org_name VARCHAR,
            account_name VARCHAR,
            account_public_id VARCHAR,
            region VARCHAR,
            total_cost FLOAT,
            date VARCHAR,
            product_name VARCHAR,
            charge_type VARCHAR,
            cost FLOAT,
            last_aggregation_function VARCHAR,
            recorded_at VARCHAR,
            partition_date DATE
        )
        """
        cursor.execute(create_table_sql)
        context.log.info(f"Table {database_name}.{schema_name}.{table_name} created/verified")

        # Delete existing data for this partition
        delete_sql = f"""
        DELETE FROM {database_name}.{schema_name}.{table_name}
        WHERE partition_date = '{start_date.isoformat()}'
        """
        cursor.execute(delete_sql)
        rows_deleted = cursor.rowcount
        context.log.info(
            f"Deleted {rows_deleted} existing rows for partition {start_date.isoformat()}"
        )

        # Insert new data using pandas
        from snowflake.connector.pandas_tools import write_pandas

        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name,
            database=database_name,
            schema=schema_name,
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False,
        )

        if not success:
            raise Exception("Failed to write data to Snowflake")

        context.log.info(
            f"Successfully inserted {nrows} rows into {database_name}.{schema_name}.{table_name}"
        )

        cursor.close()

    return MaterializeResult(
        metadata={
            "rows_inserted": nrows,
            "rows_deleted": rows_deleted,
            "snowflake_table": f"{database_name}.{schema_name}.{table_name}",
            "partition_date": start_date.isoformat(),
        }
    )


def datadog_estimated_usage(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    datadog_billing: DatadogBillingResource,
) -> MaterializeResult:
    """Fetch Datadog estimated usage data and write to Snowflake.

    This function fetches estimated cost data from Datadog's billing APIs
    for the current month, and overwrites all existing data
    in the table.

    Args:
        context: Dagster execution context
        snowflake: Snowflake resource for database connection
        datadog_billing: Datadog billing resource for API authentication

    Returns:
        MaterializeResult with metadata about the operation
    """
    # Calculate current month and previous month dates
    today = date.today()
    current_month_start = today.replace(day=1)

    # Calculate previous month start
    if current_month_start.month == 1:
        previous_month_start = date(current_month_start.year - 1, 12, 1)
    else:
        previous_month_start = date(current_month_start.year, current_month_start.month - 1, 1)

    # Format dates as YYYY-MM-DD for API (first day of each month)
    # end_month should be the first day of the month after current month to include current month data
    if current_month_start.month == 12:
        next_month_start = date(current_month_start.year + 1, 1, 1)
    else:
        next_month_start = date(current_month_start.year, current_month_start.month + 1, 1)

    start_month = previous_month_start.strftime("%Y-%m-01")
    end_month = next_month_start.strftime("%Y-%m-01")

    context.log.info(f"Fetching Datadog estimated usage data from {start_month} to {end_month}")

    all_rows = []

    # Fetch estimated cost data
    try:
        context.log.info("Fetching estimated cost data...")
        estimated_data = datadog_billing.get_estimated_cost(start_month, end_month)
        context.log.info(f"Received estimated cost data: {estimated_data}")

        # Process estimated cost data
        # Response structure: {"data": [{"id": "...", "attributes": {"org_name": "...", "public_id": "...", "charges": [...]}}]}
        if "data" in estimated_data:
            for org_data in estimated_data.get("data", []):
                org_id = org_data.get("id", "")
                org_attrs = org_data.get("attributes", {})
                org_name = org_attrs.get("org_name", "")
                org_public_id = org_attrs.get("public_id", org_id)
                account_name = org_attrs.get("account_name", "")
                account_public_id = org_attrs.get("account_public_id", "")
                region = org_attrs.get("region", "")
                total_cost = org_attrs.get("total_cost", 0)
                usage_date = org_attrs.get("date", "")
                charges = org_attrs.get("charges", [])

                # Handle both list of charge objects and single charge object
                if not isinstance(charges, list):
                    charges = [charges] if charges else []

                for charge in charges:
                    # Handle different charge record formats
                    if isinstance(charge, dict):
                        row = {
                            "org_public_id": org_public_id,
                            "org_name": org_name,
                            "account_name": account_name,
                            "account_public_id": account_public_id,
                            "region": region,
                            "total_cost": total_cost,
                            "date": usage_date,
                            "product_name": charge.get("product_name", ""),
                            "charge_type": charge.get("charge_type", ""),
                            "cost": charge.get("cost", 0),
                            "last_aggregation_function": (
                                str(charge.get("last_aggregation_function"))
                                if charge.get("last_aggregation_function") is not None
                                else None
                            ),
                            "recorded_at": str(context.run_id),
                        }
                        all_rows.append(row)
    except Exception as e:
        context.log.warning(f"Failed to fetch estimated cost data: {e}", exc_info=True)

    if not all_rows:
        context.log.warning("No estimated usage data retrieved from Datadog APIs")
        return MaterializeResult(
            metadata={
                "rows_inserted": 0,
                "message": "No data retrieved from APIs",
            }
        )

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)
    context.log.info(f"Prepared {len(df)} rows for Snowflake")

    # Get database and schema names
    database_name = get_database_for_environment("DATADOG")
    schema_name = get_schema_for_environment("RAW")
    table_name = "ESTIMATED_USAGE"

    context.log.info(f"Writing to {database_name}.{schema_name}.{table_name}")

    # Write to Snowflake
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        # Create schema if not exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}")

        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
            org_public_id VARCHAR,
            org_name VARCHAR,
            account_name VARCHAR,
            account_public_id VARCHAR,
            region VARCHAR,
            total_cost FLOAT,
            date VARCHAR,
            product_name VARCHAR,
            charge_type VARCHAR,
            cost FLOAT,
            last_aggregation_function VARCHAR,
            recorded_at VARCHAR
        )
        """
        cursor.execute(create_table_sql)
        context.log.info(f"Table {database_name}.{schema_name}.{table_name} created/verified")

        # Delete all existing data (overwrite behavior)
        delete_sql = f"""
        DELETE FROM {database_name}.{schema_name}.{table_name}
        """
        cursor.execute(delete_sql)
        rows_deleted = cursor.rowcount
        context.log.info(
            f"Deleted {rows_deleted} existing rows from {database_name}.{schema_name}.{table_name}"
        )

        # Insert new data using pandas
        from snowflake.connector.pandas_tools import write_pandas

        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name,
            database=database_name,
            schema=schema_name,
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False,
        )

        if not success:
            raise Exception("Failed to write data to Snowflake")

        context.log.info(
            f"Successfully inserted {nrows} rows into {database_name}.{schema_name}.{table_name}"
        )

        cursor.close()

    return MaterializeResult(
        metadata={
            "rows_inserted": nrows,
            "rows_deleted": rows_deleted,
            "snowflake_table": f"{database_name}.{schema_name}.{table_name}",
            "start_month": start_month,
            "end_month": end_month,
        }
    )
