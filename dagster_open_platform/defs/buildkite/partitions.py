import datetime

from dagster import HourlyPartitionsDefinition

buildkite_hourly_partition = HourlyPartitionsDefinition(start_date=datetime.datetime(2026, 1, 1))
