import dagster as dg
from dagster.components import definitions


@definitions
def defs() -> dg.Definitions:
    datadog_usage_job = dg.define_asset_job(
        "datadog_usage_job",
        selection=["datadog/raw/usage"],
        partitions_def=dg.MonthlyPartitionsDefinition(start_date="2025-01-01"),
    )
    return dg.Definitions(
        schedules=[dg.build_schedule_from_partitioned_job(datadog_usage_job)],
    )
