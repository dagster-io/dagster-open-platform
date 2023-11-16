from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AutoMaterializePolicy,
    MaterializeResult,
    MetadataValue,
    SourceAsset,
    asset,
)
from dagster_dbt import dbt_assets
from dagster_gcp import BigQueryResource
from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas

from ..partitions import oss_analytics_weekly_partition
from ..resources import (
    DBT_MANIFEST_PATH,
    dbt_resource,
)
from ..utils.dbt import CustomDagsterDbtTranslator
from ..utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)

NON_EMPTY_CHECK_NAME = "non_empty_etl"
SAME_ROWS_CHECK_NAME = "same_rows_across_bq_and_sf"


@asset(
    compute_kind="Snowflake",
    group_name="oss_analytics",
    partitions_def=oss_analytics_weekly_partition,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    check_specs=[
        AssetCheckSpec(NON_EMPTY_CHECK_NAME, asset="dagster_pypi_downloads"),
        AssetCheckSpec(SAME_ROWS_CHECK_NAME, asset="dagster_pypi_downloads"),
    ],
)
def dagster_pypi_downloads(
    context: AssetExecutionContext,
    bigquery: BigQueryResource,
    snowflake: SnowflakeResource,
):
    """A table containing the number of PyPi downloads for each package in the Dagster ecosystem, aggregated at the weekly grain. This data is fetched from the public BigQuery dataset `bigquery-public-data.pypi.file_downloads`."""
    start_week = str(context.asset_partitions_time_window_for_output().start.date())
    end_week = str(context.asset_partitions_time_window_for_output().end.date())

    database = get_database_for_environment()
    schema = get_schema_for_environment("oss_analytics")
    table_name = "dagster_pypi_downloads"

    query = f"""
        select
            date_trunc(date(timestamp), week) as `week`,
            file.project as `package`,
            count(*) as num_downloads,
        from `bigquery-public-data.pypi.file_downloads`
        where starts_with(file.project, 'dagster')
            and date(timestamp) >= parse_date('%F', '{start_week}')
            and date(timestamp) < parse_date('%F', '{end_week}')
        group by `week`, `package`
    """

    with bigquery.get_client() as client:
        df = client.query(query).to_dataframe()

    context.log.info(f"Fetched {len(df)} rows from BigQuery")

    with snowflake.get_connection() as conn:
        # for backfills and re-execution, delete all existing data for the given time window
        delete_query = f"""
            delete from {database}.{schema}.{table_name}
            where week >= '{start_week}'
            and week < '{end_week}';
        """

        try:
            conn.cursor().execute(delete_query)

            context.log.info(f"Deleted existing data between {start_week} and {end_week}")

            success, number_chunks, rows_inserted, output = write_pandas(
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

    non_empty_check_result = AssetCheckResult(
        check_name=NON_EMPTY_CHECK_NAME,
        passed=(len(df) > 0),
        metadata={"num_rows": MetadataValue.int(len(df))},
        severity=AssetCheckSeverity.WARN,
    )

    same_rows_check_results = AssetCheckResult(
        check_name=SAME_ROWS_CHECK_NAME, passed=(len(df) == rows_inserted)
    )

    return MaterializeResult(
        metadata={
            "top_downloads": MetadataValue.md(top_downloads.to_markdown()),
            "dagster_download_count": MetadataValue.int(dagster_download_count),
        },
        check_results=[non_empty_check_result, same_rows_check_results],
    )


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    select="source:prod_telemetry+",
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def oss_telemetry_dbt_assets(context: AssetExecutionContext):
    yield from dbt_resource.cli(["build"], context=context).stream()


oss_telemetry_events_raw = SourceAsset(
    key=["purina", "prod_telemetry", "oss_telemetry_events_raw"],
    description="OSS Telemetry events ingested from S3. The actual asset for this is currently in Purina until we can refactor the logic for it.",
    group_name="oss_telemetry_staging",
)
