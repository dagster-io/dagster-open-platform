from datetime import timedelta

from dagster import (
    AssetSelection,
    Definitions,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from dagster.components import definitions
from dagster_open_platform.defs.scout.assets.github_issues import github_issues

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


@definitions
def defs():
    return Definitions(
        schedules=[support_bot_schedule],
    )
