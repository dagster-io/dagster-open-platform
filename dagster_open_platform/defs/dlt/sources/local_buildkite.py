"""Local invocation of BuildKite pipeline.

USAGE

    python -m dagster_open_platform.dlt.sources.local_buildkite

    # To enable `INFO` level logging
    RUNTIME__LOG_LEVEL=INFO python -m dagster_open_platform.dlt.sources.local_buildkite


"""

from dagster_open_platform.defs.dlt.sources.buildkite import buildkite_source_v2
from dlt import pipeline
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()

    pipeline(
        pipeline_name="buildkite_pipelines",
        dataset_name="buildkite_v2",
        destination="duckdb",
    ).run(
        buildkite_source_v2(
            org_slug="dagster",
        )
    )
