import os

import boto3
from dagster import Definitions
from dagster.components import definitions
from dagster_aws.s3 import S3Resource


@definitions
def defs():
    region = os.getenv("AWS_WORKSPACE_REPLICATION_REGION")
    role_arn = os.getenv("AWS_WORKSPACE_REPLICATION_ROLE_ARN")
    if role_arn:
        creds = boto3.client("sts").assume_role(
            RoleArn=role_arn,
            RoleSessionName="dagster-workspace-replication",
            DurationSeconds=3600,
        )["Credentials"]

        return Definitions(
            resources={
                "s3_resource": S3Resource(
                    region_name=region,
                    aws_access_key_id=creds["AccessKeyId"],
                    aws_secret_access_key=creds["SecretAccessKey"],
                    aws_session_token=creds["SessionToken"],
                )
            }
        )
    else:
        return Definitions(
            resources={
                "s3_resource": S3Resource(
                    region_name=region,
                )
            }
        )
