"""Local invocation of BuildKite pipeline.

USAGE

    python -m dagster_open_platform.dlt.sources.local_buildkite

"""

from dagster_open_platform.dlt.sources.buildkite import pipelines

from dlt import pipeline

if __name__ == "__main__":
    pipeline(
        pipeline_name="buildkite_pipelines",
        dataset_name="buildkite",
        destination="snowflake",
    ).run(
        pipelines(
            org_slug="dagster",
            pipeline_slugs=["internal", "dagster"],
        )
    )
