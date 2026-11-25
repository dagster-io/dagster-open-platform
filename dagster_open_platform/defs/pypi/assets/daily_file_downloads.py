import dagster as dg
import pandas as pd
from dagster.components import definitions
from dagster_cloud.dagster_insights import InsightsBigQueryResource
from dagster_open_platform.defs.pypi.partitions import oss_analytics_daily_partition
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas

NON_EMPTY_CHECK_NAME = "non_empty_etl"
SAME_ROWS_CHECK_NAME = "same_rows_across_bq_and_sf"

daily_file_downloads_asset_key = ["bigquery", "pypi", "daily_file_downloads"]

# Package family -> name prefix; the bare family name also matches. dagster's plain
# prefix covers dagstermill, and prefect's hyphen keeps out unrelated projects.
# Keep in sync with the package_family case in pypi_daily_downloads.sql.
PACKAGE_FAMILY_PREFIXES = {"dagster": "dagster", "prefect": "prefect-"}


@dg.asset(
    tags={"dagster/kind/snowflake": ""},
    key=daily_file_downloads_asset_key,
    group_name="oss_analytics",
    partitions_def=oss_analytics_daily_partition,
    automation_condition=dg.AutomationCondition.eager(),
    check_specs=[
        dg.AssetCheckSpec(NON_EMPTY_CHECK_NAME, asset=daily_file_downloads_asset_key),
        dg.AssetCheckSpec(SAME_ROWS_CHECK_NAME, asset=daily_file_downloads_asset_key),
    ],
    # 31-day chunks keep each BigQuery scan of a multi-year backfill within memory
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=31),
)
def daily_file_downloads(
    context: dg.AssetExecutionContext,
    bigquery: InsightsBigQueryResource,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """Daily PyPI downloads of Dagster and Prefect packages by version, installer, and Python version, from `bigquery-public-data.pypi.file_downloads`."""
    start_day = str(context.partition_time_window.start.date())
    end_day = str(context.partition_time_window.end.date())

    context.log.info(f"Fetching data for {start_day} to {end_day}")

    database = get_database_for_environment("BIGQUERY")
    schema = get_schema_for_environment("PYPI")
    table_name = "DAILY_FILE_DOWNLOADS"

    package_filter = " or ".join(
        f"file.project = '{family}' or starts_with(file.project, '{prefix}')"
        for family, prefix in PACKAGE_FAMILY_PREFIXES.items()
    )
    query = f"""
        select
            date_trunc(date(timestamp), day) as `day`,
            file.project as `package`,
            file.version as `version`,
            details.installer.name as `installer`,
            details.python as `python_version`,
            count(*) as num_downloads,
        from `bigquery-public-data.pypi.file_downloads`
        where ({package_filter})
            and date(timestamp) >= parse_date('%F', '{start_day}')
            and date(timestamp) < parse_date('%F', '{end_day}')
        group by `day`, `package`, `version`, `installer`, `python_version`
    """

    with bigquery.get_client() as client:
        df = client.query(query).to_dataframe()

    context.log.info(f"Fetched {len(df)} rows from BigQuery")

    if df.empty:
        # Keep existing rows rather than clearing the window; the non-empty check flags it
        context.log.warning(f"No downloads between {start_day} and {end_day}, skipping load")
        rows_inserted = 0
    else:
        rows_inserted = _replace_window(
            context,
            snowflake,
            df,
            database=database,
            schema=schema,
            table_name=table_name,
            start_day=start_day,
            end_day=end_day,
        )

    top_downloads = (
        df.sort_values(
            "num_downloads",
            ascending=False,
        )
        .reset_index(drop=True)
        .head(10)
    )

    non_empty_check_result = dg.AssetCheckResult(
        check_name=NON_EMPTY_CHECK_NAME,
        passed=(len(df) > 0),
        metadata={"num_rows": dg.MetadataValue.int(len(df))},
        severity=dg.AssetCheckSeverity.WARN,
    )

    same_rows_check_results = dg.AssetCheckResult(
        check_name=SAME_ROWS_CHECK_NAME, passed=(len(df) == rows_inserted)
    )

    return dg.MaterializeResult(
        metadata={
            "top_downloads": dg.MetadataValue.md(top_downloads.to_markdown() or ""),
            **{
                f"{family}_download_count": dg.MetadataValue.int(
                    int(
                        df.loc[
                            df["package"].eq(family) | df["package"].str.startswith(prefix),
                            "num_downloads",
                        ].sum()
                    )
                )
                for family, prefix in PACKAGE_FAMILY_PREFIXES.items()
            },
        },
        check_results=[non_empty_check_result, same_rows_check_results],
    )


def _replace_window(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
    df: pd.DataFrame,
    *,
    database: str,
    schema: str,
    table_name: str,
    start_day: str,
    end_day: str,
) -> int:
    """Replace the rows for ``[start_day, end_day)`` with ``df``; returns rows inserted."""
    table = f"{database}.{schema}.{table_name}"
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                select count(*) as table_exists
                from {database}.information_schema.tables
                where table_catalog = '{database}'
                and table_schema = '{schema}'
                and table_name = '{table_name}';
            """)
            result = cursor.fetchone()

            if result is None or result[0] == 0:
                context.log.info(f"Table {table} does not exist yet, creating it")
                _success, _number_chunks, rows_inserted, _output = write_pandas(
                    conn,
                    df,
                    table_name,
                    database=database,
                    schema=schema,
                    auto_create_table=True,
                    quote_identifiers=False,
                )
                return rows_inserted

            # write_pandas DDL commits open transactions, so stage rows in a temp table
            load_table_name = f"{table_name}_LOAD"
            cursor.execute(
                f"create temporary table {database}.{schema}.{load_table_name} like {table}"
            )
            write_pandas(
                conn,
                df,
                load_table_name,
                database=database,
                schema=schema,
                quote_identifiers=False,
            )
            columns = ", ".join(df.columns)
            cursor.execute("begin")
            cursor.execute(f"delete from {table} where day >= '{start_day}' and day < '{end_day}'")
            cursor.execute(f"""
                insert into {table} ({columns})
                select {columns} from {database}.{schema}.{load_table_name}
            """)
            rows_inserted = cursor.rowcount or 0
            cursor.execute("commit")
            context.log.info(
                f"Replaced {rows_inserted} rows in {table} for {start_day} to {end_day}"
            )
            return rows_inserted
        except Exception as e:
            context.log.error(f"Error inserting data into {table}")
            context.log.error(e)
            conn.rollback()
            raise e


@definitions
def defs():
    oss_telemetry_events_raw = dg.AssetSpec(
        key=["purina", "prod_telemetry", "oss_telemetry_events_raw"],
        description="OSS Telemetry events ingested from S3. The actual asset for this is currently in Purina until we can refactor the logic for it.",
        group_name="telemetry",
    )
    return dg.Definitions(assets=[oss_telemetry_events_raw, daily_file_downloads])
