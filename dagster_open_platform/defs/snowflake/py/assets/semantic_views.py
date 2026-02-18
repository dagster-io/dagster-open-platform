"""Dagster assets that materialize Snowflake Semantic Views from OSI YAML definitions.

To add a new semantic view, simply drop an OSI-format YAML file into:
    dagster_open_platform_dbt/semantic/

No Python changes are required - this module auto-discovers all YAML files in that
directory and generates one Dagster asset per file at load time.
"""

from collections.abc import Generator
from pathlib import Path

import dagster as dg
import yaml
from dagster.components import definitions
from dagster_open_platform.lib.snowflake.osi_translator import (
    osi_yaml_to_snowflake_semantic_view_ddl,
)
from dagster_open_platform.utils.environment_helpers import get_database_for_environment
from dagster_snowflake import SnowflakeResource

log = dg.get_dagster_logger()

# Root of the dbt project - use .resolve() to handle any symlinks or editable installs.
# semantic_views.py lives at: dagster_open_platform/defs/snowflake/py/assets/semantic_views.py
# dagster_open_platform_dbt/ is a sibling of the dagster_open_platform/ package dir,
# so we need to walk up 6 levels from this file to reach the repo root.
_DBT_PROJECT_ROOT = Path(__file__).joinpath(*[".."] * 6, "dagster_open_platform_dbt").resolve()
_SEMANTIC_DIR = _DBT_PROJECT_ROOT / "semantic"

# Logical Snowflake location — used for asset keys (always canonical, never clone names).
# At runtime, _get_target_database() resolves to the PR clone DB in branch deployments
# so semantic views are never accidentally created in production DWH_REPORTING.
_LOGICAL_DATABASE = "DWH_REPORTING"
_TARGET_SCHEMA = "SEMANTIC"


def _get_target_database() -> str:
    """Return the Snowflake database to write to.

    In branch deployments returns the PR clone (e.g. DWH_REPORTING_CLONE_12345).
    In production returns DWH_REPORTING.
    """
    return get_database_for_environment(_LOGICAL_DATABASE)


def _osi_yaml_to_asset_spec(osi_path: Path) -> dg.AssetSpec:
    """Build an AssetSpec from an OSI YAML file without executing any DDL."""
    try:
        raw = yaml.safe_load(osi_path.read_text())
        models = raw.get("semantic_model", [])
        if not models:
            raise ValueError(f"No semantic_model entries in {osi_path.name}")
        model = models[0]
        view_name: str = model["name"]
    except Exception as e:
        raise ValueError(f"Failed to parse OSI file {osi_path.name}: {e}") from e
    description: str = model.get("description", "").strip()
    description = " ".join(description.split())  # collapse newlines

    return dg.AssetSpec(
        key=dg.AssetKey([_LOGICAL_DATABASE.lower(), _TARGET_SCHEMA.lower(), view_name]),
        group_name="semantic_views",
        description=(
            f"{description}\n\nGenerated from {osi_path.relative_to(_DBT_PROJECT_ROOT)} (OSI format)."
            if description
            else f"Snowflake Semantic View generated from {osi_path.name} (OSI format)."
        ),
        tags={"dagster/kind/snowflake": ""},
        metadata={"osi_yaml_path": dg.MetadataValue.path(str(osi_path))},
    )


# Build specs at module load time by scanning the semantic/ directory.
# Each .yaml file becomes one output of the multi_asset.
_osi_yaml_files: list[Path] = sorted(_SEMANTIC_DIR.glob("*.yaml"))
_semantic_view_specs: list[dg.AssetSpec] = [_osi_yaml_to_asset_spec(p) for p in _osi_yaml_files]


@dg.multi_asset(
    name="snowflake_semantic_views",
    specs=_semantic_view_specs,
    description="Materializes Snowflake Semantic Views from OSI YAML files in dagster_open_platform_dbt/semantic/.",
    can_subset=True,
)
def snowflake_semantic_views(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Generator[dg.MaterializeResult, None, None]:
    """Execute CREATE OR REPLACE SEMANTIC VIEW for each requested OSI YAML.

    Because can_subset=True, Dagster may run only a subset of views (e.g. when
    a single file changes). context.selected_asset_keys tells us which ones to run.
    """
    target_database = _get_target_database()
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {target_database}.{_TARGET_SCHEMA};")
        cur.execute(f"USE DATABASE {target_database};")
        cur.execute(f"USE SCHEMA {target_database}.{_TARGET_SCHEMA};")

        for asset_key in context.selected_asset_keys:
            spec = context.assets_def.specs_by_key[asset_key]
            osi_path = Path(spec.metadata["osi_yaml_path"].value)
            view_name = asset_key.path[-1]

            ddl = osi_yaml_to_snowflake_semantic_view_ddl(
                osi_yaml_path=osi_path,
                target_database=target_database,
                target_schema=_TARGET_SCHEMA,
            )

            log.info(f"Creating/replacing semantic view {view_name}:\n{ddl}")
            cur.execute(ddl)
            log.info(f"Done: {target_database}.{_TARGET_SCHEMA}.{view_name}")

            yield dg.MaterializeResult(
                asset_key=asset_key,
                metadata={
                    "osi_yaml": dg.MetadataValue.path(str(osi_path)),
                    "semantic_view": dg.MetadataValue.text(
                        f"{target_database}.{_TARGET_SCHEMA}.{view_name}"
                    ),
                    "ddl": dg.MetadataValue.md(f"```sql\n{ddl}\n```"),
                },
            )


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(assets=[snowflake_semantic_views])
