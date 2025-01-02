import dagster as dg
from dagster_dbt import DbtManifestAssetSelection
from dagster_open_platform.dbt.assets import CustomDagsterDbtTranslator, dbt_snapshot_models
from dagster_open_platform.dbt.partitions import insights_partition
from dagster_open_platform.dbt.resources import dagster_open_platform_dbt_project
from dagster_open_platform.utils.environment_helpers import get_database_asset_key_for_environment, get_schema_for_environment

######################################################
##              INSIGHTS                            ##
######################################################

UNMATCHED_SNOWFLAKE_SUBMISSION_ASSETS = dg.AssetSelection.assets(
    # Do not build these models along with the insights job, since they are
    # heuristics that identify potential pipeline issues rather than strict
    # correctness checks.
    dg.AssetKey(
        [get_database_asset_key_for_environment().lower(), get_schema_for_environment("cloud_reporting").lower(), "reporting_unmatched_snowflake_cost_observation_metadata"]
    ),
    dg.AssetKey(
        [get_database_asset_key_for_environment().lower(), get_schema_for_environment("cloud_reporting").lower(), "reporting_unmatched_user_submitted_snowflake_cost_metrics"]
    ),
)

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
        # Do not run these checks along with the insights job, since they are
        # heuristics that identify potential pipeline issues rather than strict
        # correctness checks.
        - UNMATCHED_SNOWFLAKE_SUBMISSION_ASSETS
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


insights_snowflake_submission_checks_schedule = dg.ScheduleDefinition(
    cron_schedule="30 1 * * *",
    job=dg.define_asset_job(
        "insights_snowflake_submission_checks_job",
        selection=UNMATCHED_SNOWFLAKE_SUBMISSION_ASSETS,
        tags={"team": "insights", "dbt_pipeline": "insights"},
    ),
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


dbt_analytics_snapshot_sensor = dg.AutomationConditionSensorDefinition(
    "dbt_analytics_snapshot_sensor",
    target=dg.AssetSelection.assets(dbt_snapshot_models),
    default_condition=dg.AutomationCondition.on_cron("0 * * * *"),
    use_user_code_server=True,
)

scheduled_jobs = [insights_job, dbt_analytics_core_job]

schedules = [
    insights_schedule,
    dbt_analytics_core_schedule,
    insights_snowflake_submission_checks_schedule,
]

sensors = [dbt_analytics_snapshot_sensor]
