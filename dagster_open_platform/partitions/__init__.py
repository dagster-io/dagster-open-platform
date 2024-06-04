from dagster import TimeWindowPartitionsDefinition, WeeklyPartitionsDefinition
from dagster._utils.partitions import DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE

# split up every 3 hrs
insights_partition = TimeWindowPartitionsDefinition(
    start="2023-08-23-00:00",
    cron_schedule="0 */3 * * *",
    fmt=DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE,
)

oss_analytics_weekly_partition = WeeklyPartitionsDefinition(start_date="2019-01-01")
