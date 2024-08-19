from dagster import (
    DynamicPartitionsDefinition,
    SensorEvaluationContext,
    SensorResult,
    _check as check,
    sensor,
)
from dagster_aws.s3 import S3Resource
from dagster_open_platform.aws.constants import BUCKET_NAME, INPUT_PREFIX
from dagster_open_platform.aws.utils import S3Mailman

org_partitions_def = DynamicPartitionsDefinition(name="organizations")


@sensor(description="Sensor to detect new organizations in workspace replication")
def organization_sensor(context: SensorEvaluationContext, s3_resource: S3Resource):
    # clear_org_partitions(context)
    s3_client = s3_resource.get_client()
    s3_mailman = S3Mailman(
        bucket_name=BUCKET_NAME,
        input_prefix=INPUT_PREFIX,
        output_prefix="",
        s3_client=s3_client,
    )
    contents = s3_mailman.get_contents(get_all=True)
    org_ids = [content["Key"].split("/")[3] for content in contents]

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
