from dagster import DailyPartitionsDefinition, WeeklyPartitionsDefinition

insights_partition = DailyPartitionsDefinition(start_date="2023-08-23")

oss_analytics_weekly_partition = WeeklyPartitionsDefinition(start_date="2019-01-01")
