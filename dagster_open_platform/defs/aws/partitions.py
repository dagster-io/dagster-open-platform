import dagster as dg

BATCH_SIZE = 10

daily_partition_def = dg.DailyPartitionsDefinition(start_date="2024-08-14")
org_partitions_def = dg.DynamicPartitionsDefinition(name="organizations")
