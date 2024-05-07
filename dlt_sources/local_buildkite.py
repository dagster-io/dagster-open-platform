"""Local invocation of BuildKite pipeline.

USAGE

    python -m dlt_sources.local_buildkite

"""

from dlt import pipeline

from dlt_sources.buildkite import pipelines

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
