"""Local invocation of GitHub issues pipeline.

USAGE

    python -m dlt_sources.local_github


"""

import os

import yaml
from dlt import pipeline

from dlt_sources.github import github_reactions

if __name__ == "__main__":
    cwd = os.path.dirname(os.path.abspath(__file__))
    dlt_configuration_path = os.path.join(cwd, "dlt_configuration.yaml")
    dlt_configuration = yaml.safe_load(open(dlt_configuration_path))

    dlt_source = github_reactions(
        dlt_configuration["sources"]["github"]["repositories"],
        items_per_page=100,
        max_items=1000,
    )

    dlt_pipeline = pipeline(
        pipeline_name="github_issues",
        dataset_name="github",
        destination="snowflake",
    )

    dlt_pipeline.run(dlt_source)
