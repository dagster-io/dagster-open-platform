import os
from pathlib import Path

from dagster import EnvVar
from dagster_cloud.dagster_insights import InsightsBigQueryResource
from dagster_dbt import DbtCliResource, DbtProject
from dagster_slack import SlackResource
from dagster_snowflake import SnowflakeResource

from ..utils.environment_helpers import get_dbt_target
from .hightouch_resource import ConfigurableHightouchResource
from .scoutos_resource import GithubResource, ScoutosResource
from .sling_resource import CustomSlingResource, SlingPostgresConfig, SlingSnowflakeConfig

bigquery_resource = InsightsBigQueryResource(
    gcp_credentials=EnvVar("GCP_CREDENTIALS"),
)

snowflake_resource = SnowflakeResource(
    user=EnvVar("SNOWFLAKE_USER"),
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE", "PURINA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PURINA"),
)

dagster_open_platform_dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "dagster_open_platform_dbt").resolve(),
    target=get_dbt_target(),
)

dbt_resource = DbtCliResource(project_dir=dagster_open_platform_dbt_project)

slack_resource = SlackResource(token=EnvVar("SLACK_ANALYTICS_TOKEN"))

cloud_prod_sling_resource = CustomSlingResource(
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
hightouch_resource = ConfigurableHightouchResource(api_key=EnvVar("HIGHTOUCH_API_KEY"))
