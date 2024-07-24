from dagster import Definitions, SourceAsset

from ..utils.source_code import add_code_references_and_link_to_git

segment_asset_keys = [
    ["segment", "dagster_io", "identifies"],
    ["segment", "dagster_io", "pages"],
    ["segment", "dagster_cloud", "tracks"],
]

segment_source_assets = [
    SourceAsset(
        key=key,
        description="A table containing Segment data loaded using a Snowflake integration.",
        group_name="segment",
    )
    for key in segment_asset_keys
]

defs = Definitions(
    assets=add_code_references_and_link_to_git(segment_source_assets),
)
