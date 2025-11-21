"""Compass definitions for Dagster Plus table exports."""

from dagster import Definitions, EnvVar
from dagster.components import definitions
from dagster_gcp import BigQueryResource

from .py.assets import (
    compass_dagster_plus_tables_bigquery_load,
    compass_dagster_plus_tables_gcs_export,
)


@definitions
def defs() -> Definitions:
    """Define Compass assets for exporting Dagster Plus tables to GCS and BigQuery."""
    return Definitions(
        assets=[
            compass_dagster_plus_tables_gcs_export,
            compass_dagster_plus_tables_bigquery_load,
        ],
        resources={
            "bigquery_compass_prospector": BigQueryResource(
                project="compass-prospector",
                location="US",
                gcp_credentials=EnvVar("COMPASS_PROSPECTOR_GCP_CREDENTIALS"),
            ),
        },
    )
