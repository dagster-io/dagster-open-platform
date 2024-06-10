from datetime import timedelta

from dagster import (
    AssetSelection,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from dagster_open_platform.assets import monitor_purina_clones, support_bot
from dagster_open_platform.assets.oss_analytics import dagster_pypi_downloads

support_bot_job = define_asset_job(
    name="support_bot_job",
    selection=AssetSelection.assets(support_bot.github_issues),
    tags={"team": "devrel"},
)


######################################################
##              ASK AI Support BOT                  ##
######################################################


@schedule(job=support_bot_job, cron_schedule="@daily")
def support_bot_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    return RunRequest(
        tags={
            ASSET_PARTITION_RANGE_START_TAG: (
                context.scheduled_execution_time - timedelta(days=30)
            ).strftime("%Y-%m-%d"),
            ASSET_PARTITION_RANGE_END_TAG: context.scheduled_execution_time.strftime("%Y-%m-%d"),
        }
    )


######################################################
##              Purina Cleanup                      ##
######################################################

purina_clone_cleanup_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="purina_clone_cleanup_job",
        selection=[monitor_purina_clones.inactive_snowflake_clones],
        tags={"team": "devrel"},
    ),
    cron_schedule="0 3 * * *",
)

######################################################
##              OSS Telemetry Ingest + dbt          ##
######################################################

oss_analytics_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="oss_analytics_job",
        selection=(AssetSelection.assets(dagster_pypi_downloads).downstream(include_self=False)),
        tags={"team": "devrel"},
    ),
    cron_schedule="0 * * * *",
)

scheduled_jobs = [support_bot_job]


schedules = [
    purina_clone_cleanup_schedule,
    support_bot_schedule,
    oss_analytics_schedule,
]
