import json

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    Output,
    multi_asset,
)
from dagster_open_platform.aws.constants import (
    BUCKET_NAME,
    DAGSTER_OBJECTS,
    INPUT_PREFIX,
    OUTPUT_PREFIX,
    session,
)
from dagster_open_platform.aws.sensors import org_partitions_def


@multi_asset(
    group_name="aws",
    specs=[
        AssetSpec(key=AssetKey([BUCKET_NAME, "raw", "workspace", "metadata"])),
        AssetSpec(key=AssetKey([BUCKET_NAME, "raw", "workspace", "repo_metadata"])),
        AssetSpec(key=AssetKey([BUCKET_NAME, "raw", "workspace", "external_repo_metadata"])),
        *[
            AssetSpec(key=AssetKey([BUCKET_NAME, "raw", "workspace", dag_obj]))
            for dag_obj in DAGSTER_OBJECTS.values()
        ],
    ],
    partitions_def=MultiPartitionsDefinition(
        {
            "date": DailyPartitionsDefinition(start_date="2024-08-14"),
            "organization": org_partitions_def,
        }
    ),
)
def workspace_data_json(context: AssetExecutionContext):
    s3_client = session.client("s3")

    date, org = context.partition_key.split("|")
    bucket = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{INPUT_PREFIX}/{date}/{org}/")

    for obj_info in bucket.get("Contents", []):
        key = obj_info.get("Key", "")
        output_key_ending = "/".join(key.split("/")[2:])

        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        obj_dict = json.loads(obj["Body"].read().decode("utf-8"))

        # Pull the repo datas out of the object
        repository_datas = obj_dict.pop("repository_datas")

        metadata_output_key = "/".join([OUTPUT_PREFIX, "metadata", output_key_ending])
        s3_client.put_object(
            Bucket=BUCKET_NAME, Key=metadata_output_key, Body=json.dumps(obj_dict).encode("utf-8")
        )

        for repository_data in repository_datas:
            # Pull external repo datas out of the repository data
            external_repository_data = repository_data.pop("external_repository_data")

            repo_name = external_repository_data.get("repo_name", "__repository__")
            repo_metadata_output_key = "/".join(
                [OUTPUT_PREFIX, "repo_metadata", output_key_ending, repo_name]
            )
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=repo_metadata_output_key,
                Body=json.dumps(repository_data).encode("utf-8"),
            )

            for dagster_object_key, dagster_object_name in DAGSTER_OBJECTS.items():
                if dagster_object_key in external_repository_data:
                    dagster_object = external_repository_data.pop(dagster_object_key)
                    if dagster_object:
                        for i, item in enumerate(dagster_object):
                            dagster_object_output_key = "/".join(
                                [
                                    OUTPUT_PREFIX,
                                    dagster_object_name,
                                    output_key_ending,
                                    repo_name,
                                    str(i + 1),
                                ]
                            )
                            s3_client.put_object(
                                Bucket=BUCKET_NAME,
                                Key=dagster_object_output_key,
                                Body=json.dumps(item).encode("utf-8"),
                            )

            external_repo_metadata_output_key = "/".join(
                [OUTPUT_PREFIX, "external_repo_metadata", output_key_ending, repo_name]
            )
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=external_repo_metadata_output_key,
                Body=json.dumps(external_repository_data).encode("utf-8"),
            )

    yield Output(None, output_name=f"{BUCKET_NAME}__raw__workspace__metadata")
    yield Output(None, output_name=f"{BUCKET_NAME}__raw__workspace__repo_metadata")
    yield Output(None, output_name=f"{BUCKET_NAME}__raw__workspace__external_repo_metadata")
    for dag_obj in DAGSTER_OBJECTS.values():
        yield Output(None, output_name=f"{BUCKET_NAME}__raw__workspace__{dag_obj}")
