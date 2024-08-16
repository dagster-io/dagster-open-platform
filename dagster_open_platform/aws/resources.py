import os

from dagster_aws.s3 import S3Resource

s3_resource = S3Resource(
    aws_access_key_id=os.getenv("AWS_WORKSPACE_REPLICATION_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_WORKSPACE_REPLICATION_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_WORKSPACE_REPLICATION_REGION"),
)
