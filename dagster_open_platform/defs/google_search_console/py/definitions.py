from dagster import Definitions, EnvVar
from dagster.components import definitions

from .assets import google_search_console_search_analytics
from .resources import GoogleSearchConsoleResource
from .schedules import google_search_console_daily_schedule


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[google_search_console_search_analytics],
        schedules=[google_search_console_daily_schedule],
        resources={
            "google_search_console": GoogleSearchConsoleResource(
                service_account_json_base64=EnvVar(
                    "GOOGLE_SEARCH_CONSOLE_SERVICE_ACCOUNT_JSON_BASE64"
                ),
                site_url=EnvVar("GOOGLE_SEARCH_CONSOLE_SITE_URL"),
            ),
        },
    )
