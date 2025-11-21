"""Compass definitions for Dagster Plus table exports."""

from dagster import Definitions
from dagster.components import definitions

from .py.assets import compass_dagster_plus_tables_gcs_export


@definitions
def defs() -> Definitions:
    """Define Compass assets for exporting Dagster Plus tables to GCS."""
    return Definitions(
        assets=[compass_dagster_plus_tables_gcs_export],
    )
