import contextlib
from datetime import timedelta
from pathlib import Path

import pytest
from dagster import (
    AssetKey,
    AssetsDefinition,
    AutomationCondition,
    build_asset_context,
    deserialize_value,
)
from dagster._core.definitions.asset_spec import (
    INTERNAL_FRESHNESS_POLICY_METADATA_KEY,
    InternalFreshnessPolicy,
)
from dagster._core.test_utils import environ
from dagster_open_platform.utils.components_test import build_component_and_defs

from dagster_open_platform_tests.utils import component_context


@pytest.mark.parametrize("environment", ["dev", "prod"])
def test_load_common_room_activities(environment: str) -> None:
    with environ({"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod" if environment == "prod" else ""}):
        expected_schema = "elementl" if environment == "prod" else "dev"

        component, defs = build_component_and_defs(
            component_context(Path("snowflake/assets/common_room")),
        )

        assets_def = next(iter(defs.assets or []))
        assert isinstance(assets_def, AssetsDefinition)

        assert assets_def.keys == {
            AssetKey(["aws", expected_schema, "stage_common_room_activities"]),
            AssetKey(["aws", expected_schema, "stage_common_room_community_members"]),
            AssetKey(["aws", expected_schema, "stage_common_room_groups"]),
        }

        stage_common_room_activities_spec = assets_def.specs_by_key[
            AssetKey(["aws", expected_schema, "stage_common_room_activities"])
        ]
        assert deserialize_value(
            stage_common_room_activities_spec.metadata[INTERNAL_FRESHNESS_POLICY_METADATA_KEY]
        ) == (
            InternalFreshnessPolicy.time_window(
                fail_window=timedelta(hours=23),
            )
        )
        assert stage_common_room_activities_spec.group_name == "aws_stages"
        assert (
            stage_common_room_activities_spec.automation_condition
            == AutomationCondition.on_cron("0 3 * * *")
        )


@pytest.mark.parametrize("environment", ["dev", "prod"])
@pytest.mark.parametrize("does_entity_exist", [True, False])
def test_run_common_room_activities(environment: str, does_entity_exist: bool) -> None:
    with environ({"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod" if environment == "prod" else ""}):
        expected_schema = "elementl" if environment == "prod" else "dev"
        executed_queries = []

        class MockCursor:
            def execute(self, query: str):
                executed_queries.append(query)

            def fetchall(self):
                return ["value"] if does_entity_exist else []

        class MockSnowflake:
            @contextlib.contextmanager
            def get_connection(self):
                yield self

            def cursor(self):
                return MockCursor()

        component, defs = build_component_and_defs(
            component_context(Path("snowflake/assets/common_room")),
        )

        assets_def = next(iter(defs.assets or []))
        assert isinstance(assets_def, AssetsDefinition)

        result = list(assets_def(context=build_asset_context(), snowflake_sf=MockSnowflake()))  # type: ignore

        # 3 assets -> 3 outputs
        assert len(result) == 3

        assert len(executed_queries) == 10
        assert executed_queries[0] == "USE ROLE AWS_WRITER;"
        assert executed_queries[1] == f"USE SCHEMA AWS.{expected_schema.upper()};"

        assert "SHOW STAGES LIKE 'stage_common_room_activities';" in executed_queries
        assert "SHOW STAGES LIKE 'stage_common_room_community_members';" in executed_queries
        assert "SHOW STAGES LIKE 'stage_common_room_groups';" in executed_queries

        if does_entity_exist:
            assert "ALTER STAGE stage_common_room_activities REFRESH;" in executed_queries, str(
                executed_queries
            )
            assert "ALTER STAGE stage_common_room_community_members REFRESH;" in executed_queries, (
                str(executed_queries)
            )
            assert "ALTER STAGE stage_common_room_groups REFRESH;" in executed_queries, str(
                executed_queries
            )
        else:
            assert any(
                query.startswith("CREATE STAGE stage_common_room_activities")
                for query in executed_queries
            ), "CREATE STAGE query should be in executed_queries"
            assert any(
                query.startswith("CREATE STAGE stage_common_room_community_members")
                for query in executed_queries
            ), "CREATE STAGE query should be in executed_queries"
            assert any(
                query.startswith("CREATE STAGE stage_common_room_groups")
                for query in executed_queries
            ), "CREATE STAGE query should be in executed_queries"
