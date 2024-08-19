import os

BUCKET_NAME = os.getenv("AWS_WORKSPACE_REPLICATION_BUCKET_NAME", "")
ACCOUNT_NAME = os.getenv("AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME", "")
INPUT_PREFIX = "raw/workspace"
OUTPUT_PREFIX = "staging"
DAGSTER_OBJECTS = {
    "external_asset_graph_data": "assets",
    "external_asset_checks": "asset_checks",
    "external_partition_set_datas": "partitions",
    "external_pipeline_datas": "pipelines",
    "external_schedule_datas": "schedules",
    "external_sensor_datas": "sensors",
    "external_resource_data": "resources",
    "external_job_refs": "jobs",
}
DAGSTER_OBJECT_CHUNK_SIZE = 150
DAGSTER_METADATA_OBJECTS = [
    "metadata",
    "repo_metadata",
    "external_repo_metadata",
]


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
