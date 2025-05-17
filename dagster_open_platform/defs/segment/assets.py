from dagster import AssetSpec, Definitions
from dagster.components import definitions
from dagster_open_platform.definitions import global_freshness_policy


@definitions
def defs():
    segment_asset_keys = [
        ["segment", "dagster_io", "identifies"],
        ["segment", "dagster_io", "pages"],
        ["segment", "dagster_io", "hubspot_form_submit"],
        ["segment", "dagster_cloud", "accounts"],
        ["segment", "dagster_cloud", "identifies"],
        ["segment", "dagster_cloud", "pages"],
        ["segment", "dagster_cloud", "tracks"],
        ["segment", "dagster_cloud", "trial_start"],
        ["segment", "dagster_cloud", "users"],
        ["segment", "dagster_university", "certificate_record_downloaded"],
        ["segment", "dagster_university", "course_landing_page"],
        ["segment", "dagster_university", "identifies"],
        ["segment", "dagster_university", "pages"],
        ["segment", "dagster_university", "quiz_start_page"],
        ["segment", "dagster_university", "signup"],
        ["segment", "dagster_university", "tracks"],
        ["segment", "dagster_university", "users"],
        ["segment", "dagster_university", "viewed_iframe"],
    ]

    return Definitions(
        assets=[
            AssetSpec(
                key=key,
                description="A table containing Segment data loaded using a Snowflake integration.",
                group_name="segment",
                kinds={"segment"},
                internal_freshness_policy=global_freshness_policy,
            )
            for key in segment_asset_keys
        ]
    )
