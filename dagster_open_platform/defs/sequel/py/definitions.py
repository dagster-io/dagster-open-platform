import os

from dagster import Definitions, EnvVar
from dagster.components import definitions
from dagster_snowflake import SnowflakeResource

from .assets import (
    sequel_events_full_refresh,
    sequel_registrants_full_refresh,
    sequel_user_activity_full_refresh,
)
from .resources import SequelResource
from .schedules import (
    sequel_event_logs_schedule,
    sequel_events_schedule,
    sequel_registrants_schedule,
)


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[
            sequel_events_full_refresh,
            sequel_registrants_full_refresh,
            sequel_user_activity_full_refresh,
        ],
        schedules=[
            sequel_events_schedule,
            sequel_registrants_schedule,
            sequel_event_logs_schedule,
        ],
        resources={
            "sequel": SequelResource(
                client_id=EnvVar("SOURCES__SEQUEL__CLIENT_ID"),
                client_secret=EnvVar("SOURCES__SEQUEL__CLIENT_SECRET"),
                company_id=EnvVar("SOURCES__SEQUEL__COMPANY_ID"),
            ),
            "snowflake_sequel": SnowflakeResource(
                user=EnvVar("SNOWFLAKE_USER"),
                account=EnvVar("SNOWFLAKE_ACCOUNT"),
                role=os.getenv("SNOWFLAKE_ROLE", "PURINA"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PURINA"),
                # Comment this line in production
                authenticator="externalbrowser",
                # Uncomment these lines in production
                # password=EnvVar("SNOWFLAKE_PASSWORD"),
                # additional_snowflake_connection_args={"authenticator": "username_password_mfa"},
            ),
        },
    )
