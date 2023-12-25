from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    MaterializeResult,
    asset,
)
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource

BASE_S3_LOCATION = (
    "@PURINA.PUBLIC.s3_elementl_cloud_cost_stage/CostStandardExport//CostStandardExport/data"
)
CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS {QUALIFIED_TABLE_NAME} USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'{BASE_S3_LOCATION}/BILLING_PERIOD=2023-12/CostStandardExport-00001.snappy.parquet'
          , FILE_FORMAT=>'PURINA.PUBLIC.PARQUET_FORMAT'
          )
        )
);
"""

COPY_DATA_QUERY = """
COPY INTO {QUALIFIED_TABLE_NAME}
    FROM '{BASE_S3_LOCATION}/'
    FILE_FORMAT=(FORMAT_NAME='PURINA.PUBLIC.PARQUET_FORMAT')
    PATTERN='.*CostStandardExport.*[.]parquet'
    FORCE=TRUE
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
"""

TRUNCATE_TABLE_QUERY = "TRUNCATE TABLE {QUALIFIED_TABLE_NAME};"

materialize_on_cron_policy = AutoMaterializePolicy.eager().with_rules(
    AutoMaterializeRule.materialize_on_cron("0 */4 * * *"),
)


@asset(auto_materialize_policy=materialize_on_cron_policy)
def aws_cost_report(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """AWS updates the monthly cost report once an hour, overwriting the existing
    files for the current month.

    To keep things simple, we truncate the table and reload the data every time
    in case of corrections.
    """
    database = get_database_for_environment()
    schema = get_schema_for_environment("FINANCE")
    table_name = "aws_cost_report"
    qualified_name = ".".join([database, schema, table_name])

    create_table = CREATE_TABLE_QUERY.format(
        QUALIFIED_TABLE_NAME=qualified_name,
        BASE_S3_LOCATION=BASE_S3_LOCATION,
    )
    copy_table = COPY_DATA_QUERY.format(
        QUALIFIED_TABLE_NAME=qualified_name,
        BASE_S3_LOCATION=BASE_S3_LOCATION,
    )
    truncate_table = TRUNCATE_TABLE_QUERY.format(
        QUALIFIED_TABLE_NAME=qualified_name,
    )

    with snowflake.get_connection() as conn:
        conn.autocommit(False)
        cur = conn.cursor()
        try:
            cur.execute(create_table)
            context.log.info(f"Table {qualified_name} successfully created")
            cur.execute(truncate_table)
            context.log.info(f"Table {qualified_name} successfully truncated")
            cur.execute(copy_table)
            context.log.info(f"Data successfully copied into {qualified_name}")
            cur.execute(f"SELECT COUNT(*) FROM {qualified_name}")
            rows = cur.fetchone()[0]  # type: ignore
            context.log.info(f"{rows} rows inserted into {qualified_name}")
        except Exception as e:
            conn.rollback()
            context.log.error(f"Error loading data into {qualified_name}")
            raise e

    return MaterializeResult(
        metadata={
            "snowflake_table": qualified_name,
            "rows_inserted": rows,
        }
    )
