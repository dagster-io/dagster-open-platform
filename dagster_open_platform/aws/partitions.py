from dagster import DailyPartitionsDefinition, DynamicPartitionsDefinition

daily_partition_def = DailyPartitionsDefinition(start_date="2024-08-14")
org_partitions_def = DynamicPartitionsDefinition(name="organizations")
