from dagster_open_platform.defs.dlt.sources.thinkific import thinkific
from dlt import pipeline

thinkific_source = thinkific()
thinkific_pipeline = pipeline(
    pipeline_name="thinkific",
    dataset_name="thinkific",
    destination="snowflake",
    progress="log",
)
