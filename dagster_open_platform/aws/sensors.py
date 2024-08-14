import os

import boto3
import dagster._check as check
from dagster import DynamicPartitionsDefinition, SensorEvaluationContext, SensorResult, sensor
from dagster_open_platform.aws.constants import BUCKET_NAME

org_partitions_def = DynamicPartitionsDefinition(name="organizations")


@sensor(description="Sensor to detect new organizations in workspace replication")
def organization_sensor(context: SensorEvaluationContext):
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    s3_client = session.client("s3")

    is_truncated = True
    continuation_token = None
    org_ids = []
    while is_truncated:
        bucket = (
            s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="workspace")
            if continuation_token is None
            else s3_client.list_objects_v2(
                Bucket=BUCKET_NAME, Prefix="workspace", ContinuationToken=continuation_token
            )
        )

        keys = [check.inst(o.get("Key"), str) for o in bucket.get("Contents", [])]
        org_ids.extend([key.split("/")[1] for key in keys])

        is_truncated = bucket["IsTruncated"]
        if is_truncated:
            continuation_token = bucket["NextContinuationToken"]

    distinct_org_ids = list(set(org_ids))

    new_orgs = [
        org_id
        for org_id in distinct_org_ids
        if not org_partitions_def.has_partition_key(
            org_id, dynamic_partitions_store=context.instance
        )
    ]

    return SensorResult(
        dynamic_partitions_requests=[org_partitions_def.build_add_request(new_orgs)]
    )
