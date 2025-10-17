import dagster as dg
from dagster.components import definitions
from dagster_dbt import DbtManifestAssetSelection
from dagster_open_platform.defs.dbt.assets import (
    CustomDagsterDbtTranslator,
    get_dbt_snapshot_models,
)
from dagster_open_platform.defs.dbt.partitions import insights_partition
from dagster_open_platform.defs.dbt.resources import dagster_open_platform_dbt_project

######################################################
##              Main DBT Pipeline                   ##
######################################################


@definitions
def defs():
    dbt_analytics_core_job = dg.define_asset_job(
        name="dbt_analytics_core_job",
        selection=(
            DbtManifestAssetSelection.build(
                manifest=dagster_open_platform_dbt_project().manifest_path,
                dagster_dbt_translator=CustomDagsterDbtTranslator(),
            ).required_multi_asset_neighbors()
            # snapshot models
            - dg.AssetSelection.assets(get_dbt_snapshot_models())
        ),
        tags={
            "team": "devrel",
            "dbt_pipeline": "analytics_core",
            "dagster/max_retries": 1,
        },
    )

    @dg.schedule(cron_schedule="0 3 * * *", job=dbt_analytics_core_job)
    def dbt_analytics_core_schedule():
        most_recent_partition = insights_partition.get_last_partition_key()
        yield dg.RunRequest(
            partition_key=str(most_recent_partition), run_key=str(most_recent_partition)
        )

    dbt_analytics_snapshot_schedule = dg.ScheduleDefinition(
        job=dg.define_asset_job(
            name="dbt_analytics_snapshot_job",
            selection=dg.AssetSelection.assets(get_dbt_snapshot_models()).downstream()
            - dg.AssetSelection.kind("omni", include_sources=True),
        ),
        cron_schedule="0 7 * * *",
    )

    return dg.Definitions(schedules=[dbt_analytics_core_schedule, dbt_analytics_snapshot_schedule])
