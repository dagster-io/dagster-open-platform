from dagster import DailyPartitionsDefinition, WeeklyPartitionsDefinition

oss_analytics_weekly_partition = WeeklyPartitionsDefinition(start_date="2019-01-01")
oss_analytics_daily_partition = DailyPartitionsDefinition(start_date="2019-01-01")
