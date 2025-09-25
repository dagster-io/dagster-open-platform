import os

import dagster as dg
from dagster.components import definitions
from dagster_open_platform.utils.environment_helpers import get_environment
from dagster_snowflake import SnowflakeResource

snowflake = SnowflakeResource(
    user=dg.EnvVar("SNOWFLAKE_USER"),
    account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
    password=None if get_environment() == "LOCAL" else dg.EnvVar("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE", "PURINA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PURINA"),
    authenticator="externalbrowser" if get_environment() == "LOCAL" else "username_password_mfa",
)


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        resources={"snowflake": snowflake},
    )
