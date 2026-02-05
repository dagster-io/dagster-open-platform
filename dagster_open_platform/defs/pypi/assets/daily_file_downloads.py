import dagster as dg
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

dagster_pypi_downloads_asset_key = ["bigquery", "pypi", "daily_file_downloads"]


@dg.asset(
    tags={"dagster/kind/snowflake": ""},
    key=dagster_pypi_downloads_asset_key,
    group_name="oss_analytics",
    partitions_def=oss_analytics_daily_partition,
    automation_condition=dg.AutomationCondition.eager(),
    check_specs=[
        dg.AssetCheckSpec(NON_EMPTY_CHECK_NAME, asset=dagster_pypi_downloads_asset_key),
        dg.AssetCheckSpec(SAME_ROWS_CHECK_NAME, asset=dagster_pypi_downloads_asset_key),
    ],
    backfill_policy=dg.BackfillPolicy.single_run(),
)
def dagster_pypi_downloads(
    context: dg.AssetExecutionContext,
    bigquery: InsightsBigQueryResource,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """A table containing the number of PyPi downloads for each package in the Dagster ecosystem, aggregated at the weekly grain. This data is fetched from the public BigQuery dataset `bigquery-public-data.pypi.file_downloads`."""
    start_day = str(context.partition_time_window.start.date())
    end_day = str(context.partition_time_window.end.date())

    context.log.info(f"Fetching data for {start_day} to {end_day}")

    database = get_database_for_environment("BIGQUERY")
    schema = get_schema_for_environment("PYPI")
    table_name = "DAILY_FILE_DOWNLOADS"

    query = f"""
        select
            date_trunc(date(timestamp), day) as `day`,
            file.project as `package`,
            file.version as `version`,
            details.installer.name as `installer`,
            details.python as `python_version`,
            count(*) as num_downloads,
        from `bigquery-public-data.pypi.file_downloads`
        where starts_with(file.project, 'dagster')
            and date(timestamp) >= parse_date('%F', '{start_day}')
            and date(timestamp) < parse_date('%F', '{end_day}')
        group by `day`, `package`, `version`, `installer`, `python_version`
    """

    with bigquery.get_client() as client:
        df = client.query(query).to_dataframe()

    context.log.info(f"Fetched {len(df)} rows from BigQuery")

    with snowflake.get_connection() as conn:
        # for backfills and re-execution, delete all existing data for the given time window
        # Only delete if the table exists
        delete_query = f"""
            delete from {database}.{schema}.{table_name}
            where day >= '{start_day}'
            and day < '{end_day}';
        """

        try:
            # Check if table exists before attempting delete
            check_table_query = f"""
                select count(*) as table_exists
                from {database}.information_schema.tables
                where table_catalog = '{database}'
                and table_schema = '{schema}'
                and table_name = '{table_name}';
            """

            cursor = conn.cursor()
            cursor.execute(check_table_query)
            result = cursor.fetchone()
            table_exists = result is not None and result[0] > 0

            if table_exists:
                cursor.execute(delete_query)
                context.log.info(f"Deleted existing data between {start_day} and {end_day}")
            else:
                context.log.info(
                    f"Table {database}.{schema}.{table_name} does not exist yet, skipping delete"
                )

            _success, _number_chunks, rows_inserted, _output = write_pandas(
                conn,
                df,
                table_name,
                database=database,
                schema=schema,
                auto_create_table=True,
                overwrite=False,
                quote_identifiers=False,
            )

            context.log.info(f"Inserted {rows_inserted} rows into {database}.{schema}.{table_name}")
        except Exception as e:
            context.log.error(f"Error inserting data into {database}.{schema}.{table_name}")
            context.log.error(e)
            conn.rollback()
            raise e

    top_downloads = (
        df.sort_values(
            "num_downloads",
            ascending=False,
        )
        .reset_index(drop=True)
        .head(10)
    )

    dagster_download_count = int(df[df["package"] == "dagster"]["num_downloads"].values[0])

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
            "top_downloads": dg.MetadataValue.md(top_downloads.to_markdown()),
            "dagster_download_count": dg.MetadataValue.int(dagster_download_count),
        },
        check_results=[non_empty_check_result, same_rows_check_results],
    )


@definitions
def defs():
    oss_telemetry_events_raw = dg.AssetSpec(
        key=["purina", "prod_telemetry", "oss_telemetry_events_raw"],
        description="OSS Telemetry events ingested from S3. The actual asset for this is currently in Purina until we can refactor the logic for it.",
        group_name="telemetry",
    )
    return dg.Definitions(assets=[oss_telemetry_events_raw, dagster_pypi_downloads])
