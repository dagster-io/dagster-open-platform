from dagster import Definitions, EnvVar
from dagster.components import definitions
from dagster_open_platform.defs.google_drive.py.jobs import anthropic_api_costs_to_drive
from dagster_open_platform.defs.google_drive.py.resources import GoogleDriveResource
from dagster_open_platform.defs.google_drive.py.schedules import anthropic_api_costs_weekly_schedule


@definitions
def defs() -> Definitions:
    return Definitions(
        jobs=[anthropic_api_costs_to_drive],
        schedules=[anthropic_api_costs_weekly_schedule],
        resources={
            "google_drive": GoogleDriveResource(
                service_account_json_base64=EnvVar("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON_BASE64"),
            ),
        },
    )
