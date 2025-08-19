from datetime import timedelta

from dagster import RunRequest, ScheduleDefinition, ScheduleEvaluationContext
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)

from .assets import google_search_console_search_analytics


def google_search_console_schedule_function(context: ScheduleEvaluationContext):
    """Schedule function that creates run requests for the last 4 days of partitions."""
    return RunRequest(
        tags={
            ASSET_PARTITION_RANGE_START_TAG: (
                context.scheduled_execution_time - timedelta(days=4)
            ).strftime("%Y-%m-%d-00:00"),
            ASSET_PARTITION_RANGE_END_TAG: context.scheduled_execution_time.strftime(
                "%Y-%m-%d-00:00"
            ),
        }
    )


google_search_console_daily_schedule = ScheduleDefinition(
    name="google_search_console_daily_schedule",
    cron_schedule="0 2 * * *",  # Run daily at 2 AM UTC
    execution_fn=google_search_console_schedule_function,
    target=google_search_console_search_analytics,
    description="Daily schedule that processes the last 4 days of Google Search Console data",
)
