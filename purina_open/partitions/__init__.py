from dagster import WeeklyPartitionsDefinition

oss_analytics_weekly_partition = WeeklyPartitionsDefinition(start_date="2019-01-01")
