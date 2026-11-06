from collections.abc import Generator
from datetime import timedelta

import dagster as dg
from dagster import AutomationCondition
from dagster_snowflake import SnowflakeResource

from dagster_open_platform.defs.buildkite.partitions import buildkite_daily_partition
from dagster_open_platform.defs.buildkite.resources import BuildkiteResource
from dagster_open_platform.defs.buildkite.utils import BuildkiteSQL

_buildkite_raw_automation_condition = AutomationCondition.on_cron("0 23 * * *")


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key="buildkite_builds",
            group_name="buildkite",
            tags={"dagster/kind/buildkite": "", "dagster/kind/snowflake": ""},
            description="One row per Buildkite build across all pipelines and branches.",
            automation_condition=_buildkite_raw_automation_condition,
        ),
        dg.AssetSpec(
            key="buildkite_jobs",
            group_name="buildkite",
            tags={"dagster/kind/buildkite": "", "dagster/kind/snowflake": ""},
            description="One row per job across all Buildkite builds.",
            automation_condition=_buildkite_raw_automation_condition,
        ),
    ],
    partitions_def=buildkite_daily_partition,
)
def buildkite_raw(
    context: dg.AssetExecutionContext,
    buildkite: BuildkiteResource,
    snowflake: SnowflakeResource,
) -> Generator[dg.MaterializeResult]:
    """Fetches all finished Buildkite builds (and their jobs) for the partition day
    and loads them into Snowflake. Runs daily with a 3-hour lookback buffer to capture
    builds from the previous partition that were still in-flight during the last run.
    """
    window = context.partition_time_window
    # the buildkite api query only gets finished jobs, so this is to catch builds from the
    # previous partition that were still in progress when that partition was materialized.
    # the database insert should be idempotent, so fetching duplicate jobs from bk should be fine.
    created_from = (window.start - timedelta(hours=3)).isoformat()
    created_to = window.end.isoformat()

    context.log.info(
        "Fetching finished builds created_from=%s created_to=%s",
        created_from,
        created_to,
    )
    builds = buildkite.get_builds(
        created_from=created_from,
        created_to=created_to,
        state="finished",
    )
    jobs = [j for b in builds for j in b.jobs]
    context.log.info("Fetched %d builds and %d jobs", len(builds), len(jobs))

    buildkite_sql = BuildkiteSQL(snowflake)

    buildkite_sql.insert_builds(builds)
    context.log.info("Inserted %d builds", len(builds))
    yield dg.MaterializeResult(
        asset_key="buildkite_builds",
        metadata={
            "num_builds": dg.MetadataValue.int(len(builds)),
            "partition_start": created_from,
            "partition_end": created_to,
        },
    )

    buildkite_sql.insert_jobs(jobs)
    context.log.info("Inserted %d jobs", len(jobs))
    yield dg.MaterializeResult(
        asset_key="buildkite_jobs",
        metadata={
            "num_jobs": dg.MetadataValue.int(len(jobs)),
            "partition_start": created_from,
            "partition_end": created_to,
        },
    )


BUILDKITE_HEX_DASHBOARD_URL = "https://app.hex.tech/455a1b0e-b799-4046-a886-2f1798e9dbc0/app/Buildkite-4nrzydkNDokgxxxQfSOaJF/latest"

buildkite_builds_hex_dashboard = dg.AssetSpec(
    key="buildkite_hex_dashboard",
    group_name="buildkite",
    kinds={"hex"},
    deps=["buildkite_builds", "buildkite_jobs"],
    description=f"Hex dashboard visualizing Buildkite metrics: {BUILDKITE_HEX_DASHBOARD_URL}",
    metadata={"url": dg.MetadataValue.url(BUILDKITE_HEX_DASHBOARD_URL)},
)
