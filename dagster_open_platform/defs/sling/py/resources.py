from dagster import Definitions, EnvVar
from dagster.components import definitions
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_environment,
    get_schema_for_environment,
)
from dagster_sling import SlingConnectionResource, SlingResource

embedded_elt_resource = SlingResource(
    # Ignores are necessary due to known issue with pyright + permissiveconfigs
    connections=[
        SlingConnectionResource(
            name="CLOUD_PRODUCTION_MAIN",
            type="postgres",
            host=EnvVar("CLOUD_PROD_REPLICA_POSTGRES_TAILSCALE_HOST"),  # type: ignore
            user=EnvVar("CLOUD_PROD_POSTGRES_USER"),  # type: ignore
            database="dagster",  # type: ignore
            password=EnvVar("CLOUD_PROD_POSTGRES_PASSWORD"),  # type: ignore
            sslmode="require",  # type: ignore
        ),
        SlingConnectionResource(
            name="SLING_DB_MAIN",
            type="snowflake",
            host=EnvVar("SLING_SNOWFLAKE_ACCOUNT"),  # type: ignore
            user=EnvVar("SNOWFLAKE_SLING_USER"),  # type: ignore
            private_key=EnvVar("SNOWFLAKE_SLING_PRIVATE_KEY"),  # type: ignore
            database="sandbox" if get_environment() != "PROD" else "sling",  # type: ignore
            schema=get_schema_for_environment("cloud_product"),  # type: ignore
            warehouse="purina",  # type: ignore
            role="purina" if get_environment() != "PROD" else "sling",  # type: ignore
        ),
        SlingConnectionResource(
            name="CLOUD_PRODUCTION_SHARD1",
            type="postgres",
            host=EnvVar("CLOUD_PROD_SHARD1_REPLICA_POSTGRES_TAILSCALE_HOST"),  # type: ignore
            user=EnvVar("CLOUD_PROD_POSTGRES_USER"),  # type: ignore
            database="shard1",  # type: ignore
            password=EnvVar("CLOUD_PROD_POSTGRES_PASSWORD"),  # type: ignore
            sslmode="require",  # type: ignore
        ),
        SlingConnectionResource(
            name="SLING_DB_SHARD1",
            type="snowflake",
            host=EnvVar("SLING_SNOWFLAKE_ACCOUNT"),  # type: ignore
            user=EnvVar("SNOWFLAKE_SLING_USER"),  # type: ignore
            private_key=EnvVar("SNOWFLAKE_SLING_PRIVATE_KEY"),  # type: ignore
            database="sandbox" if get_environment() != "PROD" else "sling",  # type: ignore
            schema=get_schema_for_environment("cloud_product_shard1"),  # type: ignore
            warehouse="purina",  # type: ignore
            role="purina" if get_environment() != "PROD" else "sling",  # type: ignore
        ),
        SlingConnectionResource(
            name="PURINA_CLOUD_REPORTING",
            type="snowflake",
            host=EnvVar("SLING_SNOWFLAKE_ACCOUNT"),  # type: ignore
            user=EnvVar("SNOWFLAKE_SLING_USER"),  # type: ignore
            private_key=EnvVar("SNOWFLAKE_SLING_PRIVATE_KEY"),  # type: ignore
            database=get_database_for_environment(),  # type: ignore
            schema=get_schema_for_environment("sales"),  # type: ignore
            warehouse="purina",  # type: ignore
            role="purina" if get_environment() != "PROD" else "sling",  # type: ignore
        ),
        SlingConnectionResource(
            name="REPORTING_DB",
            type="postgres",
            host=EnvVar("CLOUD_PROD_REPORTING_POSTGRES_TAILSCALE_HOST"),  # type: ignore
            user=EnvVar("CLOUD_PROD_POSTGRES_USER"),  # type: ignore
            database="dagster",  # type: ignore
            schema="public",  # type: ignore
            password=EnvVar("CLOUD_PROD_REPORTING_POSTGRES_PASSWORD"),  # type: ignore
            sslmode="require",  # type: ignore
        ),
    ]
)


@definitions
def defs():
    return Definitions(
        resources={"embedded_elt": embedded_elt_resource},
    )
