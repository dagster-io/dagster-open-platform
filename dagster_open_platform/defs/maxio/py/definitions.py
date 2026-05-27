from dagster import Definitions, EnvVar
from dagster.components import definitions
from dagster_open_platform.defs.maxio.py.jobs import maxio_arr_correction
from dagster_open_platform.defs.maxio.py.resources import MaxioResource
from dagster_open_platform.defs.maxio.py.schedules import maxio_arr_correction_daily_schedule
from dagster_slack import SlackResource


@definitions
def defs() -> Definitions:
    return Definitions(
        jobs=[maxio_arr_correction],
        schedules=[maxio_arr_correction_daily_schedule],
        resources={
            "maxio": MaxioResource(
                api_token=EnvVar("MAXIO_API_TOKEN"),
            ),
            "slack_notifications": SlackResource(
                token=EnvVar("SLACK_ELEMENTL_ETL_BOT_TOKEN"),
            ),
        },
    )
