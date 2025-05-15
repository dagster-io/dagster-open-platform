from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job
from dagster.components import definitions

sling_egress_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="sling_egress_job",
        selection=AssetSelection.groups("sling_egress"),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)


@definitions
def defs():
    return Definitions(
        schedules=[sling_egress_schedule],
    )
