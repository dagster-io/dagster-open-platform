from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from dagster import AssetsDefinition, DailyPartitionsDefinition, Definitions, materialize
from dagster.components.testing import get_component_defs_within_project


def test_basic_execute():
    # need to import here to avoid double import
    from dagster_open_platform.lib.executable_component import ExecutableComponent

    component, defs = get_component_defs_within_project(
        project_root=Path(__file__).parent.parent.parent,
        component_path="gong/component",
    )

    import dagster_open_platform.lib.executable_component

    assert type(component).__name__ == "ExecutableComponent"
    assert type(component) is dagster_open_platform.lib.executable_component.ExecutableComponent
    assert isinstance(component, ExecutableComponent), f"Component is {type(component)}"

    assert component.get_resource_keys() == {"claude", "snowflake", "slack_gong"}

    defs = Definitions.merge(
        defs,
        Definitions(
            resources=dict(
                snowflake=MagicMock(),
                claude=MagicMock(),
                slack_gong=MagicMock(),
            )
        ),
    )

    assets_def = defs.get_assets_def("gong_calls_transcript_ai")
    assert isinstance(assets_def, AssetsDefinition)

    assert isinstance(assets_def.partitions_def, DailyPartitionsDefinition)

    mock_df = pd.DataFrame(
        {
            "CONVERSATION_ID": ["123", "456"],
            "CALL_ANALYSIS_CONTEXT": [
                "Call between John and Jane discussing product features",
                "Call between Mike and Sarah about pricing",
            ],
        }
    )

    with patch(
        "dagster_open_platform.defs.gong.component.gong_calls_transcript_ai.get_completed_calls_with_transcripts",
        return_value=mock_df,
    ):
        with patch(
            "dagster_open_platform.defs.gong.component.gong_calls_transcript_ai.create_call_analysis_context",
            return_value=pd.DataFrame(),
        ):
            result = materialize(
                [assets_def],
                partition_key="2025-04-14",
                resources=defs.resources,
            )

            assert result.success
