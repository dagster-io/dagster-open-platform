import dagster as dg
from dagster_open_platform.defs.dbt.partitions import insights_partition

# Note: Assets are imported in definitions.py, not here to avoid circular imports

# Daily schedule for Anthropic usage data - runs at 1 AM UTC
anthropic_usage_daily_schedule = dg.build_schedule_from_partitioned_job(
    dg.define_asset_job(
        name="anthropic_usage_daily_job",
        selection=dg.AssetSelection.keys("anthropic_usage_report"),
        partitions_def=insights_partition,
        description="Daily ingestion of Anthropic usage data",
    ),
)

# Daily schedule for Anthropic cost data - runs at 2 AM UTC
anthropic_cost_daily_schedule = dg.build_schedule_from_partitioned_job(
    dg.define_asset_job(
        name="anthropic_cost_daily_job",
        selection=dg.AssetSelection.keys("anthropic_cost_report"),
        partitions_def=insights_partition,
        description="Daily ingestion of Anthropic cost data",
    ),
)
