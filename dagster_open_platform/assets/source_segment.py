from dagster import SourceAsset

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
