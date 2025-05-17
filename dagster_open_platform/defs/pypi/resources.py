from dagster import Definitions, EnvVar
from dagster.components import definitions
from dagster_cloud.dagster_insights import InsightsBigQueryResource


@definitions
def defs():
    return Definitions(
        resources={
            "bigquery": InsightsBigQueryResource(
                gcp_credentials=EnvVar("GCP_CREDENTIALS"),
            )
        }
    )
