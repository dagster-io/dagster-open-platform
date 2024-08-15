import os

import boto3

BUCKET_NAME = os.getenv("WORKSPACE_REPLICATION_BUCKET_NAME", "")
INPUT_PREFIX = "raw/workspace"
OUTPUT_PREFIX = "staging"
DAGSTER_OBJECTS = {
    "external_asset_graph_data": "assets",
    "external_asset_checks": "asset_checks",
    "external_partition_set_datas": "partitions",
    "external_pipeline_datas": "pipelines",
    "external_schedule_datas": "schedules",
    "external_sensor_datas": "sensors",
    "external_resource_data": "resources",
    "external_job_refs": "jobs",
}
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)
