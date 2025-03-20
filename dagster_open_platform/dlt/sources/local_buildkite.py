"""Local invocation of BuildKite pipeline.

USAGE

    python -m dagster_open_platform.dlt.sources.local_buildkite

"""

from dagster_open_platform.dlt.sources.buildkite import buildkite_source

from dlt import pipeline

if __name__ == "__main__":
    pipeline(
        pipeline_name="buildkite_pipelines",
        dataset_name="buildkite",
        destination="duckdb",
    ).run(
        buildkite_source(
            org_slug="dagster",
        )
    )
