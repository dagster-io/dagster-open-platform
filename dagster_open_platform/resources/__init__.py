import os

from dagster import EnvVar
from dagster_snowflake import SnowflakeResource

from .sling_resource import CustomSlingResource, SlingPostgresConfig, SlingSnowflakeConfig

snowflake_resource = SnowflakeResource(
    user=EnvVar("SNOWFLAKE_USER"),
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE", "PURINA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PURINA"),
)

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
