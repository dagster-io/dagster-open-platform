from dagster import AssetExecutionContext, BackfillPolicy, asset
from dagster_open_platform.defs.dbt.partitions import insights_partition
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource

from .resources import GoogleSearchConsoleResource


@asset(
    key=["google_search_console", "search_analytics", "query_metrics"],
    group_name="google_search_console",
    description="Fetch Google Search Console data and load into Snowflake with clicks > 0 filter",
    partitions_def=insights_partition,
    backfill_policy=BackfillPolicy.single_run(),
)
def google_search_console_search_analytics(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    google_search_console: GoogleSearchConsoleResource,
) -> None:
    """Fetch Google Search Console search analytics data for a single day
    and load into Snowflake. Filters for queries with clicks > 0.
    """
    # Get the partition date range
    partition_window = context.partition_time_window
    start_date = partition_window.start.date()
    end_date = partition_window.end.date()

    # Query Google Search Console API with pagination to get ALL rows
    context.log.info(f"Fetching GSC data from {start_date} to {end_date}")

    all_rows = []
    start_row = 0
    row_limit = 25000  # Maximum allowed by Google Search Console API

    while True:
        rows = google_search_console.search_analytics_query_with_pagination(
            dimensions=["query", "date"],
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            search_type="web",
            data_state="final",
            row_limit=row_limit,
            start_row=start_row,
        )

        if not rows:
            break

        all_rows.extend(rows)
        context.log.info(f"Fetched batch starting at row {start_row}, got {len(rows)} rows")

        # If we got fewer rows than the limit, we've reached the end
        if len(rows) < row_limit:
            break

        start_row += row_limit

    context.log.info(f"Total rows fetched: {len(all_rows)}")

    # Process and filter data
    processed_data = []
    for row in all_rows:
        keys = row.get("keys", [])
        if len(keys) >= 2:
            query = keys[0]
            date = keys[1]
            clicks = row.get("clicks", 0)
            impressions = row.get("impressions", 0)

            # Filter for clicks > 0
            if clicks > 0:
                processed_data.append(
                    {"date": date, "query": query, "clicks": clicks, "impressions": impressions}
                )

    context.log.info(f"Filtered {len(processed_data)} rows with clicks > 0")

    if not processed_data:
        context.log.info("No data with clicks > 0 found")
        return

    # Convert to DataFrame for easier handling
    # df = pd.DataFrame(processed_data)

    # Get environment-specific database and schema
    database_name = get_database_for_environment("GOOGLE_SEARCH_CONSOLE")
    schema_name = get_schema_for_environment("SEARCH_ANALYTICS")
    table_name = "QUERY_METRICS"

    # Write to Snowflake
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
                date DATE,
                query STRING,
                clicks INTEGER,
                impressions INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
            """
            cursor.execute(create_table_sql)

            # Delete existing data for the partition date range to avoid duplicates
            delete_sql = f"""
            DELETE FROM {database_name}.{schema_name}.{table_name}
            WHERE date >= '{start_date}' AND date <= '{end_date}'
            """
            cursor.execute(delete_sql)

            # Insert new data
            insert_sql = f"""
            INSERT INTO {database_name}.{schema_name}.{table_name} (date, query, clicks, impressions)
            VALUES (%s, %s, %s, %s)
            """

            # Prepare data for insertion
            data_to_insert = [
                (row["date"], row["query"], row["clicks"], row["impressions"])
                for row in processed_data
            ]

            cursor.executemany(insert_sql, data_to_insert)

            context.log.info(
                f"Inserted {len(data_to_insert)} rows into {database_name}.{schema_name}.{table_name}"
            )
