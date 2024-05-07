"""Local invocation of BuildKite pipeline.

USAGE

    python -m dlt_sources.local_buildkite

"""

from dlt import pipeline

from dlt_sources.buildkite import pipelines

if __name__ == "__main__":
    dlt_pipeline = pipeline(
        pipeline_name="buildkite_pipelines",
        dataset_name="buildkite",
        destination="snowflake",
    )

    dlt_pipeline.run(
        pipelines(
            org_slug="dagster",
            pipeline_slug="internal",
        )
    )
