from dagster import ScheduleDefinition
from dagster_open_platform.defs.google_drive.py.jobs import anthropic_api_costs_to_drive

anthropic_api_costs_weekly_schedule = ScheduleDefinition(
    job=anthropic_api_costs_to_drive,
    cron_schedule="0 9 * * 1",
    execution_timezone="America/New_York",
    description="Weekly export of ANTHROPIC_API_COSTS from Snowflake to Google Drive.",
)
