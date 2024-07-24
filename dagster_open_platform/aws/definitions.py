from datetime import datetime

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    Definitions,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    asset,
)
from dagster_open_platform.snowflake.resources import snowflake_resource
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource

from ..utils.source_code import add_code_references_and_link_to_git

# note: this is used as 2 different formats
# YYYY-MM for sourcing the partition from s3
# or YYYY-MM-DD for querying fromSnowflake where DD is "01"
# PARTITION_MONTH = "<calling code sets the partition>"
BASE_S3_LOCATION = (
    "@PURINA.PUBLIC.s3_elementl_cloud_cost_stage/CostStandardExport/"
    "/CostStandardExport/data/BILLING_PERIOD={PARTITION_MONTH}"
)

# QUALIFIED_TABLE_NAME = "<calling code sets the table name>"
CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS {QUALIFIED_TABLE_NAME} USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'{BASE_S3_LOCATION}/CostStandardExport-00001.snappy.parquet'
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

DELETE_PARTITION_QUERY = (
    "DELETE FROM {QUALIFIED_TABLE_NAME} "
    "WHERE to_varchar(\"bill_billing_period_start_date\",'YYYY-MM-DD') = '{PARTITION_MONTH}';"
)

aws_monthly_partition = MonthlyPartitionsDefinition(start_date="2020-12-01", end_offset=1)

materialize_on_cron_policy = AutoMaterializePolicy.eager().with_rules(
    AutoMaterializeRule.materialize_on_cron("0 */4 * * *"),
)


@asset(partitions_def=aws_monthly_partition, auto_materialize_policy=materialize_on_cron_policy)
def aws_cost_report(context: AssetExecutionContext, snowflake_aws: SnowflakeResource):
    """AWS updates the monthly cost report once an hour, overwriting the existing
    files for the current month.

    """
    database = get_database_for_environment()
    schema = get_schema_for_environment("FINANCE")
    table_name = "aws_cost_report"
    qualified_name = ".".join([database, schema, table_name])

    partition = context.partition_key
    partition_no_day = datetime.strptime(partition, "%Y-%m-%d").strftime("%Y-%m")

    create_table = CREATE_TABLE_QUERY.format(
        QUALIFIED_TABLE_NAME=qualified_name, BASE_S3_LOCATION=BASE_S3_LOCATION
    ).format(PARTITION_MONTH=partition_no_day)
    context.log.info(f"SQL debug {create_table}")
    copy_table = COPY_DATA_QUERY.format(
        QUALIFIED_TABLE_NAME=qualified_name, BASE_S3_LOCATION=BASE_S3_LOCATION
    ).format(PARTITION_MONTH=partition_no_day)
    context.log.info(f"SQL debug {copy_table}")
    delete_partition = DELETE_PARTITION_QUERY.format(
        QUALIFIED_TABLE_NAME=qualified_name,
        PARTITION_MONTH=partition,
    )
    context.log.info(f"SQL debug {delete_partition}")

    with snowflake_aws.get_connection() as conn:
        conn.autocommit(False)

        cur = conn.cursor()
        try:
            cur.execute(create_table)
            context.log.info(f"Table {qualified_name} successfully created")
            cur.execute(delete_partition)
            context.log.info(f"Table {qualified_name} partition {partition} successfully deleted")
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


defs = Definitions(
    assets=add_code_references_and_link_to_git([aws_cost_report]),
    resources={
        "snowflake_aws": snowflake_resource,
    },
)
