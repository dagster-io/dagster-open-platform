import os

from dagster import EnvVar, file_relative_path
from dagster_dbt import DbtCliResource
from dagster_gcp import BigQueryResource
from dagster_slack import SlackResource
from dagster_snowflake import SnowflakeResource

from ..utils.environment_helpers import get_dbt_target
from .scoutos_resource import GithubResource, ScoutosResource
from .sling_resource import SlingPostgresConfig, SlingResource, SlingSnowflakeConfig
from .stitch_resource import StitchResource

DBT_MANIFEST_PATH = file_relative_path(__file__, "../../dbt/target/manifest.json")

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
    project_dir=file_relative_path(__file__, "../../dbt"),
    profiles_dir=file_relative_path(__file__, "../../dbt"),
    target=get_dbt_target(),
)

stitch_resource = StitchResource(
    stitch_client_id=EnvVar("STITCH_CLIENT_ID"),
    access_token=EnvVar("STITCH_ACCESS_TOKEN"),
)

slack_resource = SlackResource(token=EnvVar("SLACK_ANALYTICS_TOKEN"))

cloud_prod_reporting_sling_resource = SlingResource(
    postgres_config=SlingPostgresConfig(
        host=EnvVar("CLOUD_PROD_REPORTING_POSTGRES_HOST"),
        user=EnvVar("CLOUD_PROD_POSTGRES_USER"),
        database="dagster",
        password=EnvVar("CLOUD_PROD_REPORTING_POSTGRES_PASSWORD"),
        ssh_tunnel=EnvVar("CLOUD_PROD_BASTION_URI"),
        ssh_private_key=EnvVar("POSTGRES_SSH_PRIVATE_KEY"),
    ),
    snowflake_config=SlingSnowflakeConfig(
        host=EnvVar("SNOWFLAKE_PURINA_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_PURINA_USER"),
        password=EnvVar("SNOWFLAKE_PURINA_PASSWORD"),
        database="purina",
        warehouse="purina",
        role="purina",
    ),
)

cloud_prod_read_replica_sling_resource = SlingResource(
    postgres_config=SlingPostgresConfig(
        host=EnvVar("CLOUD_PROD_READ_REPLICA_POSTGRES_HOST"),
        user=EnvVar("CLOUD_PROD_POSTGRES_USER"),
        database="dagster",
        password=EnvVar("CLOUD_PROD_POSTGRES_PASSWORD"),
        ssh_tunnel=EnvVar("CLOUD_PROD_BASTION_URI"),
        ssh_private_key=EnvVar("POSTGRES_SSH_PRIVATE_KEY"),
    ),
    snowflake_config=SlingSnowflakeConfig(
        host=EnvVar("SNOWFLAKE_PURINA_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_PURINA_USER"),
        password=EnvVar("SNOWFLAKE_PURINA_PASSWORD"),
        database="purina",
        warehouse="purina",
        role="purina",
    ),
)

cloud_prod_sling_resource = SlingResource(
    postgres_config=SlingPostgresConfig(
        host=EnvVar("CLOUD_PROD_POSTGRES_HOST"),
        user=EnvVar("CLOUD_PROD_POSTGRES_USER"),
        database="dagster",
        password=EnvVar("CLOUD_PROD_POSTGRES_PASSWORD"),
        ssh_tunnel=EnvVar("CLOUD_PROD_BASTION_URI"),
        ssh_private_key=EnvVar("POSTGRES_SSH_PRIVATE_KEY"),
    ),
    snowflake_config=SlingSnowflakeConfig(
        host=EnvVar("SNOWFLAKE_PURINA_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_PURINA_USER"),
        password=EnvVar("SNOWFLAKE_PURINA_PASSWORD"),
        database="purina",
        warehouse="purina",
        role="purina",
    ),
)


github_resource = GithubResource(github_token=EnvVar("GITHUB_TOKEN"))
scoutos_resource = ScoutosResource(api_key=EnvVar("SCOUTOS_API_KEY"))
