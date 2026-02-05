import pandas as pd
import requests
from dagster import AssetExecutionContext, file_relative_path
from dagster_open_platform.utils.environment_helpers import get_environment
from dagster_snowflake import SnowflakeResource


def fetch_paginated_data(
    url: str,
    headers: dict,
    params: dict,
    context: AssetExecutionContext,
) -> tuple[list, int]:
    """Fetch paginated data from Anthropic Admin API.

    Args:
        url: The API endpoint URL
        headers: Request headers including API key
        params: Query parameters for the request
        context: Dagster execution context for logging

    Returns:
        Tuple of (all_data, page_count) where all_data is a list of all records
        across all pages and page_count is the number of pages fetched
    """
    all_data = []
    page = None
    page_count = 0

    # Handle pagination
    while True:
        if page:
            params["page"] = page

        page_count += 1
        context.log.info(f"Fetching page {page_count}")

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        for data_list in data.get("data", []):
            starting_at = data_list.get("starting_at")
            ending_at = data_list.get("ending_at")
            results = data_list.get("results", [])
            # Add starting_at and ending_at to each result dictionary
            for result in results:
                result["starting_at"] = starting_at
                result["ending_at"] = ending_at
            all_data.extend(results)

        if not data.get("has_more", False):
            break
        page = data.get("next_page")

    return all_data, page_count


def load_dataframe_to_snowflake(
    df: pd.DataFrame,
    snowflake: SnowflakeResource,
    context: AssetExecutionContext,
    create_table_sql: str,
    temp_table_name: str,
    target_table_name: str,
    delete_sql: str,
    insert_sql: str,
) -> int:
    """Load a DataFrame into Snowflake using temp table and replace pattern.

    Deletes existing data for the partition time window and inserts new data.

    Args:
        df: DataFrame to load
        snowflake: Snowflake resource for connection
        context: Dagster execution context for logging
        create_table_sql: SQL file path to create the target table if not exists
        temp_table_name: Name of the temporary table to create
        target_table_name: Name of the target table (for logging)
        delete_sql: SQL file path to delete partition data from target table
        insert_sql: SQL file path to insert data from temp table into target table

    Returns:
        Number of rows inserted into the target table
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        # Use sandbox database if environment is not prod
        current_db = "SANDBOX" if get_environment() != "PROD" else "ANTHROPIC"
        cursor.execute(f"USE DATABASE {current_db}")
        cursor.execute("USE SCHEMA RAW")

        # Create table if not exists
        fd = open(file_relative_path(__file__, create_table_sql))
        create_table_query = fd.read()
        fd.close()

        cursor.execute(create_table_query)

        # Get current database and schema
        current_db = cursor.execute("SELECT CURRENT_DATABASE()").fetchone()[0]  # type: ignore
        current_schema = cursor.execute("SELECT CURRENT_SCHEMA()").fetchone()[0]  # type: ignore

        # Write DataFrame to temp table
        from snowflake.connector.pandas_tools import write_pandas

        _success, _nchunks, nrows, _ = write_pandas(
            conn,
            df,
            temp_table_name,
            database=current_db,
            schema=current_schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )

        context.log.info(f"Wrote {nrows} rows to temp table")

        # Delete existing partition data
        fd = open(file_relative_path(__file__, delete_sql))
        delete_query = fd.read()
        fd.close()

        cursor.execute(delete_query)
        rows_deleted = cursor.rowcount
        context.log.info(f"Deleted {rows_deleted} rows from {target_table_name}")

        # Insert new data
        fd = open(file_relative_path(__file__, insert_sql))
        insert_query = fd.read()
        fd.close()

        cursor.execute(insert_query)
        rows_inserted = cursor.rowcount
        context.log.info(f"Inserted {rows_inserted} rows into {target_table_name}")

        # Drop temp table
        cursor.execute(f"DROP TABLE IF EXISTS {current_schema}.{temp_table_name}")
        context.log.info(f"Dropped temp table {temp_table_name}")

        cursor.close()

    return rows_inserted if rows_inserted is not None else 0
