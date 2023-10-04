import os

from dagster import EnvVar, file_relative_path
from dagster_dbt import DbtCliResource
from dagster_gcp import BigQueryResource
from dagster_snowflake import SnowflakeResource

from ..utils.environment_helpers import get_dbt_target

bigquery_resource = BigQueryResource(
    gcp_credentials=EnvVar("GCP_CREDENTIALS"),
)

snowflake_resource = SnowflakeResource(
    user=EnvVar("SNOWFLAKE_USER"),
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE", "PURINA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PURINA"),
)

dbt_resource = DbtCliResource(
    project_dir=file_relative_path(__file__, "../../purina_open_dbt"),
    profiles_dir=file_relative_path(__file__, "../../purina_open_dbt"),
    target=get_dbt_target(),
)
DBT_MANIFEST_PATH = file_relative_path(__file__, "../../purina_open_dbt/target/manifest.json")
