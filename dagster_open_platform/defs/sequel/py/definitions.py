from dagster import Definitions, EnvVar
from dagster.components import definitions

from .assets import (
    sequel_events_full_refresh,
    sequel_registrants_full_refresh,
    sequel_user_activity_full_refresh,
)
from .resources import SequelResource
from .schedules import (
    sequel_events_schedule,
    sequel_registrants_schedule,
    sequel_user_activity_logs_schedule,
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
            sequel_user_activity_logs_schedule,
        ],
        resources={
            "sequel": SequelResource(
                client_id=EnvVar("SOURCES__SEQUEL__CLIENT_ID"),
                client_secret=EnvVar("SOURCES__SEQUEL__CLIENT_SECRET"),
                company_id=EnvVar("SOURCES__SEQUEL__COMPANY_ID"),
            ),
        },
    )
