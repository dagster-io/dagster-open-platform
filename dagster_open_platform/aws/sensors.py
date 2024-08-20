from datetime import datetime, timedelta

from dagster import (
    SensorEvaluationContext,
    SensorResult,
    _check as check,
    sensor,
)
from dagster_aws.s3 import S3Resource
from dagster_open_platform.aws.constants import BUCKET_NAME, INPUT_PREFIX
from dagster_open_platform.aws.partitions import org_partitions_def
from dagster_open_platform.aws.utils import S3Mailman


@sensor(description="Sensor to detect new organizations in workspace replication")
def organization_sensor(context: SensorEvaluationContext, s3_resource: S3Resource):
    # clear_org_partitions(context)
    # Use cursor to check if the sensor has run before (and collected al old orgs)
    s3_client = s3_resource.get_client()
    last_date_evaluated = datetime.strptime(context.cursor, "%Y-%m-%d") if context.cursor else None

    all_org_ids = []
    if last_date_evaluated is not None:
        days_from_today = (datetime.now() - last_date_evaluated).days
        prefixes = []
        for i in range(
            -2, days_from_today
        ):  # look back at least 2 days and all days between last_date_evaluated and today
            prefix_date = (last_date_evaluated + timedelta(days=i)).strftime("%Y-%m-%d")
            prefix = f"{INPUT_PREFIX}/{prefix_date}"
            prefixes.append(prefix)

        for prefix in prefixes:
            s3_mailman = S3Mailman(
                bucket_name=BUCKET_NAME,
                input_prefix=prefix,
                output_prefix="",
                s3_client=s3_client,
            )
            contents = s3_mailman.get_contents(get_all=True)
            org_ids = [content["Key"].split("/")[3] for content in contents]
            all_org_ids.extend(org_ids)

    else:
        s3_mailman = S3Mailman(
            bucket_name=BUCKET_NAME,
            input_prefix=INPUT_PREFIX,
            output_prefix="",
            s3_client=s3_client,
        )
        contents = s3_mailman.get_contents(get_all=True)
        all_org_ids = [content["Key"].split("/")[3] for content in contents]
    context.update_cursor(datetime.now().strftime("%Y-%m-%d"))

    distinct_org_ids = list(set(all_org_ids))

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
