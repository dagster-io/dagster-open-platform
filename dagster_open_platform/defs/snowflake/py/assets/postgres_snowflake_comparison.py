import os
from datetime import date, timedelta

import dagster as dg
import pandas as pd
import psycopg2
from dagster_open_platform.defs.snowflake.py.resources import PostgresResource
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas

log = dg.get_dagster_logger()


@dg.asset(
    group_name="snowflake_py",
    description=(
        "Compares the count of asset materializations per day for the last 7 days "
        "between sling.cloud_product.event_logs (Snowflake), dagster.event_log_partitioned (Postgres source), "
        "dwh_reporting.product_by_day.organizations_by_day (business table), "
        "and purina.product.usage_metrics_daily (usage metrics). "
        "Note: The Snowflake table is replicated from Postgres via Sling."
    ),
    key=["snowflake", "py", "postgres_snowflake_comparison"],
    automation_condition=dg.AutomationCondition.cron_tick_passed("0 2 * * *"),
)
def postgres_snowflake_comparison(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    """Compares asset materialization counts per day between multiple sources.

    Sources:
    - sling.cloud_product.event_logs (Snowflake - replicated from Postgres)
    - dagster.event_log_partitioned (Postgres source)
    - dwh_reporting.product_by_day.organizations_by_day (business table - asset_materializations_billed)
    - purina.product.usage_metrics_daily (usage metrics - materializations field)

    Returns a comparison showing counts per day for the last 7 days.
    """
    # Calculate date range for last 7 days
    end_date = date.today()
    start_date = end_date - timedelta(days=7)

    # Query Postgres source tables (dagster.event_log_partitioned and shard1.event_log_partitioned)
    log.info(
        "Querying Postgres source tables (dagster.event_log_partitioned and shard1.event_log_partitioned)..."
    )
    pg_rows_main = []
    pg_rows_shard1 = []
    pg_columns = []

    # Query main database (dagster)
    with postgres.get_connection() as pg_conn:
        pg_cur = pg_conn.cursor()
        pg_query_main = """
            SELECT 
                timestamp::date as materialization_date,
                COUNT(*) as postgres_count
            FROM event_logs_partitioned
            WHERE dagster_event_type = 'ASSET_MATERIALIZATION'
                AND run_id is not null and run_id <> ''
                AND timestamp::date >= %s
                AND timestamp::date < %s
            GROUP BY timestamp::date
        """
        pg_cur.execute(pg_query_main, (start_date, end_date))
        pg_rows_main = pg_cur.fetchall()
        # Get column names from cursor description and normalize to lowercase
        if pg_cur.description:
            pg_columns = [desc[0].lower() for desc in pg_cur.description]
        pg_cur.close()

    # Query shard1 database
    # Create a temporary connection to shard1 database (uses different host)
    # Using os.environ[] to ensure it fails if the env var is not set
    shard1_host = os.environ["CLOUD_PROD_SHARD1_REPLICA_POSTGRES_TAILSCALE_HOST"]
    with psycopg2.connect(
        host=shard1_host,
        user=postgres.user,
        database="shard1",
        password=postgres.password,
        port=postgres.port,
        sslmode=postgres.sslmode,
    ) as pg_conn_shard1:
        pg_cur_shard1 = pg_conn_shard1.cursor()
        pg_query_shard1 = """
            SELECT 
                timestamp::date as materialization_date,
                COUNT(*) as postgres_count
            FROM event_logs_partitioned
            WHERE dagster_event_type = 'ASSET_MATERIALIZATION'
                AND run_id is not null and run_id <> ''
                AND timestamp::date >= %s
                AND timestamp::date < %s
            GROUP BY timestamp::date
        """
        pg_cur_shard1.execute(pg_query_shard1, (start_date, end_date))
        pg_rows_shard1 = pg_cur_shard1.fetchall()
        pg_cur_shard1.close()

    # Combine results from both databases
    pg_rows = pg_rows_main + pg_rows_shard1

    # Convert Postgres results to DataFrame and aggregate by date
    if not pg_rows:
        raise ValueError("No asset materializations found in Postgres for the last 7 days")

    pg_df = pd.DataFrame(pg_rows, columns=pg_columns)
    pg_df["materialization_date"] = pd.to_datetime(pg_df["materialization_date"]).dt.date
    # Aggregate counts by date (in case there are duplicate dates from both databases)
    pg_df = pg_df.groupby("materialization_date")["postgres_count"].sum().reset_index()

    # Query Snowflake replicated tables (sling.cloud_product.event_logs and sling.cloud_product_shard1.event_logs)
    log.info(
        "Querying Snowflake replicated tables (sling.cloud_product.event_logs and sling.cloud_product_shard1.event_logs)..."
    )
    with snowflake.get_connection() as sf_conn:
        sf_cur = sf_conn.cursor()
        sf_query = f"""
            SELECT 
                DATE(timestamp) as materialization_date,
                COUNT(*) as snowflake_count
            FROM (
                SELECT timestamp, dagster_event_type
                FROM sling.cloud_product.event_logs
                WHERE dagster_event_type = 'ASSET_MATERIALIZATION'
                    AND run_id is not null and run_id <> ''
                    AND DATE(timestamp) >= '{start_date}'
                    AND DATE(timestamp) < '{end_date}'
                UNION ALL
                SELECT timestamp, dagster_event_type
                FROM sling.cloud_product_shard1.event_logs
                WHERE dagster_event_type = 'ASSET_MATERIALIZATION'
                    AND run_id is not null and run_id <> ''
                    AND DATE(timestamp) >= '{start_date}'
                    AND DATE(timestamp) < '{end_date}'
            )
            GROUP BY DATE(timestamp)
            ORDER BY materialization_date DESC
        """
        sf_cur.execute(sf_query)
        sf_result = sf_cur.fetch_pandas_all()

    # Convert Snowflake results
    if sf_result.empty:
        raise ValueError("No asset materializations found in Snowflake for the last 7 days")

    # Normalize column names to lowercase
    sf_result.columns = [col.lower() for col in sf_result.columns]
    sf_result["materialization_date"] = pd.to_datetime(sf_result["materialization_date"]).dt.date

    # Query business table (dwh_reporting.product_by_day.organizations_by_day)
    log.info("Querying business table (dwh_reporting.product_by_day.organizations_by_day)...")
    with snowflake.get_connection() as business_conn:
        business_cur = business_conn.cursor()
        business_query = f"""
            SELECT 
                date_day as materialization_date,
                SUM(asset_materializations_billed) as business_count
            FROM dwh_reporting.product_by_day.organizations_by_day
            WHERE date_day >= '{start_date}'
                AND date_day < '{end_date}'
            GROUP BY date_day
            ORDER BY materialization_date DESC
        """
        business_cur.execute(business_query)
        business_result = business_cur.fetch_pandas_all()

    # Convert business table results
    if business_result.empty:
        raise ValueError("No asset materializations found in business table for the last 7 days")

    # Normalize column names to lowercase
    business_result.columns = [col.lower() for col in business_result.columns]
    business_result["materialization_date"] = pd.to_datetime(
        business_result["materialization_date"]
    ).dt.date

    # Query usage metrics table (purina.product.usage_metrics_daily)
    log.info("Querying usage metrics table (purina.product.usage_metrics_daily)...")
    with snowflake.get_connection() as usage_conn:
        usage_cur = usage_conn.cursor()
        usage_query = f"""
            SELECT 
                ds as materialization_date,
                SUM(materializations) as usage_metrics_count
            FROM purina.product.usage_metrics_daily
            WHERE ds >= '{start_date}'
                AND ds < '{end_date}'
            GROUP BY ds
            ORDER BY materialization_date DESC
        """
        usage_cur.execute(usage_query)
        usage_result = usage_cur.fetch_pandas_all()

    # Convert usage metrics results
    if usage_result.empty:
        raise ValueError(
            "No asset materializations found in usage metrics table for the last 7 days"
        )

    # Normalize column names to lowercase
    usage_result.columns = [col.lower() for col in usage_result.columns]
    usage_result["materialization_date"] = pd.to_datetime(
        usage_result["materialization_date"]
    ).dt.date

    # Merge Postgres, Snowflake, business table, and usage metrics results for comparison
    comparison_df = pd.merge(
        pg_df,
        sf_result,
        on="materialization_date",
        how="outer",
        suffixes=("_pg", "_sf"),
    )
    comparison_df = pd.merge(
        comparison_df,
        business_result,
        on="materialization_date",
        how="outer",
    )
    comparison_df = pd.merge(
        comparison_df,
        usage_result,
        on="materialization_date",
        how="outer",
    ).fillna(0)

    # Ensure integer types
    comparison_df["postgres_count"] = comparison_df["postgres_count"].astype(int)
    comparison_df["snowflake_count"] = comparison_df["snowflake_count"].astype(int)
    comparison_df["business_count"] = comparison_df["business_count"].astype(int)
    comparison_df["usage_metrics_count"] = comparison_df["usage_metrics_count"].astype(int)

    # Sort by date descending
    comparison_df = comparison_df.sort_values("materialization_date", ascending=False)

    if comparison_df.empty:
        raise ValueError("No asset materializations found in any source for the last 7 days")

    # Calculate summary statistics
    total_postgres = int(comparison_df["postgres_count"].sum())
    total_snowflake = int(comparison_df["snowflake_count"].sum())
    total_business = int(comparison_df["business_count"].sum())
    total_usage_metrics = int(comparison_df["usage_metrics_count"].sum())

    # Log summary
    log.info(
        f"Comparison results for last 7 days:\n"
        f"  Total materializations (Postgres): {total_postgres}\n"
        f"  Total materializations (Snowflake): {total_snowflake}\n"
        f"  Total materializations (Business): {total_business}\n"
        f"  Total materializations (Usage Metrics): {total_usage_metrics}"
    )

    # Log daily breakdown
    log.info("Daily breakdown:")
    for _, row in comparison_df.iterrows():
        log.info(
            f"  {row['materialization_date']}: "
            f"Postgres={row['postgres_count']}, "
            f"Snowflake={row['snowflake_count']}, "
            f"Business={row['business_count']}, "
            f"Usage Metrics={row['usage_metrics_count']}"
        )

    # Write results to Snowflake table
    database = get_database_for_environment("SLING")
    schema = get_schema_for_environment("METRICS")
    table_name = "BILLABLE_ASSET_MATERIALIZATION_COUNTS"

    # Prepare dataframe for writing (select only the columns we want to store)
    output_df = comparison_df[
        [
            "materialization_date",
            "postgres_count",
            "snowflake_count",
            "business_count",
            "usage_metrics_count",
        ]
    ].copy()

    # Ensure materialization_date is a date type (not datetime)
    output_df["materialization_date"] = pd.to_datetime(output_df["materialization_date"]).dt.date

    log.info(f"Writing comparison results to {database}.{schema}.{table_name}...")

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        # Create schema if not exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")

        # Create temporary table to stage the new data
        temp_table_name = f"{table_name}_TEMP"
        write_pandas(
            conn=conn,
            df=output_df,
            table_name=temp_table_name,
            database=database,
            schema=schema,
            overwrite=True,
            auto_create_table=True,
            quote_identifiers=False,
        )
        log.info(f"Wrote {len(output_df)} rows to temp table {temp_table_name}")

        # Create target table if not exists (using LIKE to copy structure from temp table)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} LIKE {database}.{schema}.{temp_table_name}
        """
        cursor.execute(create_table_sql)
        log.info(f"Table {database}.{schema}.{table_name} created/verified")

        # Merge the temporary table with the target table
        columns = output_df.columns.tolist()
        update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in columns])
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        merge_sql = f"""
        MERGE INTO {database}.{schema}.{table_name} AS target
        USING {database}.{schema}.{temp_table_name} AS source
        ON target.materialization_date = source.materialization_date
        WHEN MATCHED THEN
            UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns}) VALUES ({insert_values})
        """
        cursor.execute(merge_sql)
        rows_merged = cursor.rowcount
        log.info(f"Merged {rows_merged} rows into {database}.{schema}.{table_name}")

        # Drop the temporary table
        cursor.execute(f"DROP TABLE IF EXISTS {database}.{schema}.{temp_table_name}")
        log.info(f"Dropped temp table {temp_table_name}")

        cursor.close()

    return dg.MaterializeResult(
        metadata={
            "start_date": str(start_date),
            "end_date": str(end_date),
            "total_days": len(comparison_df),
            "total_materializations_postgres": total_postgres,
            "total_materializations_snowflake": total_snowflake,
            "total_materializations_business": total_business,
            "total_materializations_usage_metrics": total_usage_metrics,
            "snowflake_table": f"{database}.{schema}.{table_name}",
            "rows_written": len(output_df),
        }
    )
