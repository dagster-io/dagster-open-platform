from dagster import DailyPartitionsDefinition

oss_analytics_daily_partition = DailyPartitionsDefinition(start_date="2019-01-01")
