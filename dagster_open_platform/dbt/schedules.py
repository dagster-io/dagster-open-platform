import dagster as dg
from dagster_dbt import DbtManifestAssetSelection
from dagster_open_platform.dbt.assets import CustomDagsterDbtTranslator, dbt_snapshot_models
from dagster_open_platform.dbt.partitions import insights_partition
from dagster_open_platform.dbt.resources import dagster_open_platform_dbt_project

######################################################
##              INSIGHTS                            ##
######################################################

insights_job = dg.define_asset_job(
    name="insights_job",
    selection=(
        # select all insights models, and fetch upstream, including ingestion
        DbtManifestAssetSelection.build(
            manifest=dagster_open_platform_dbt_project.manifest_path,
            dagster_dbt_translator=CustomDagsterDbtTranslator(),
            select="tag:insights",
        )
        .upstream()
        .required_multi_asset_neighbors()
        - dg.AssetSelection.groups("cloud_product_main")
        - dg.AssetSelection.groups("cloud_product_shard1")
        - dg.AssetSelection.groups("fivetran_salesforce")
        - dg.AssetSelection.groups("aws_external_tables")
        - dg.AssetSelection.groups("aws_stages")
    ),
    partitions_def=insights_partition,
    tags={"team": "insights", "dbt_pipeline": "insights"},
)


@dg.schedule(cron_schedule="0 0 * * *", job=insights_job)
def insights_schedule():
    most_recent_partition = insights_partition.get_last_partition_key()
    yield dg.RunRequest(
        partition_key=str(most_recent_partition), run_key=str(most_recent_partition)
    )


######################################################
##              Main DBT Pipeline                   ##
######################################################

dbt_analytics_core_job = dg.define_asset_job(
    name="dbt_analytics_core_job",
    selection=(
        DbtManifestAssetSelection.build(
            manifest=dagster_open_platform_dbt_project.manifest_path,
            dagster_dbt_translator=CustomDagsterDbtTranslator(),
        ).required_multi_asset_neighbors()
        - dg.AssetSelection.groups(
            # insights groups
            "cloud_reporting",
        )
        # snapshot models
        - dg.AssetSelection.assets(dbt_snapshot_models)
    ),
    tags={"team": "devrel", "dbt_pipeline": "analytics_core"},
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
        selection=dg.AssetSelection.assets(dbt_snapshot_models).downstream(),
    ),
    cron_schedule="0 7 * * *",
)

scheduled_jobs = [insights_job, dbt_analytics_core_job]

schedules = [insights_schedule, dbt_analytics_core_schedule, dbt_analytics_snapshot_schedule]
