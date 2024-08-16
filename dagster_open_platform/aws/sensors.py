from dagster import (
    DynamicPartitionsDefinition,
    SensorEvaluationContext,
    SensorResult,
    _check as check,
    sensor,
)
from dagster_aws.s3 import S3Resource
from dagster_open_platform.aws.constants import BUCKET_NAME, INPUT_PREFIX

org_partitions_def = DynamicPartitionsDefinition(name="organizations")


@sensor(description="Sensor to detect new organizations in workspace replication")
def organization_sensor(context: SensorEvaluationContext, s3_resource: S3Resource):
    # clear_org_partitions(context)
    s3_client = s3_resource.get_client()

    is_truncated = True
    continuation_token = None
    org_ids = []
    while is_truncated:
        bucket = (
            s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INPUT_PREFIX)
            if continuation_token is None
            else s3_client.list_objects_v2(
                Bucket=BUCKET_NAME, Prefix=INPUT_PREFIX, ContinuationToken=continuation_token
            )
        )

        keys = [o.get("Key", "") for o in bucket.get("Contents", [])]
        org_ids.extend([key.split("/")[3] for key in keys])

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


# Used in testing, allows user to clear the org partitions
def clear_org_partitions(context: SensorEvaluationContext):
    org_ids = org_partitions_def.get_partition_keys(dynamic_partitions_store=context.instance)
    for org_id in org_ids:
        context.instance.delete_dynamic_partition(
            partitions_def_name=check.not_none(org_partitions_def.name),
            partition_key=org_id,
        )
