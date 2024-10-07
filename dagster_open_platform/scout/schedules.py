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
from dagster_open_platform.scout.assets import github_issues

support_bot_job = define_asset_job(
    name="support_bot_job",
    selection=AssetSelection.assets(github_issues),
    tags={"team": "devrel"},
)


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


ask_ai_daily_runs = ScheduleDefinition(
    job=define_asset_job(
        name="ask_ai_daily_job",
        selection=AssetSelection.keys("scoutos_app_runs"),
        tags={"team": "devrel"},
    ),
    cron_schedule="59 23 * * *",  #
)
