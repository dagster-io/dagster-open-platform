import dagster as dg

daily_partition_def = dg.DailyPartitionsDefinition(start_date="2024-08-14")
org_partitions_def = dg.DynamicPartitionsDefinition(name="organizations")

org_daily_partition_def = dg.MultiPartitionsDefinition(
    {"date": daily_partition_def, "org": org_partitions_def}
)
