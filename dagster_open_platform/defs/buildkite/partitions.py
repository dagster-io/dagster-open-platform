import datetime

from dagster import DailyPartitionsDefinition

buildkite_daily_partition = DailyPartitionsDefinition(start_date=datetime.datetime(2026, 3, 1))
