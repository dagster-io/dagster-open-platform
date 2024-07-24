from dagster import EnvVar
from dagster_cloud.dagster_insights import InsightsBigQueryResource

bigquery_resource = InsightsBigQueryResource(
    gcp_credentials=EnvVar("GCP_CREDENTIALS"),
)
