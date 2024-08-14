import json

import boto3
from dagster import AssetExecutionContext, AssetKey, AssetSpec, Output, multi_asset
from dagster_open_platform.aws.constants import BUCKET_NAME, DAGSTER_OBJECTS
from dagster_open_platform.aws.sensors import org_partitions_def


@multi_asset(
    group_name="aws",
    specs=[
        AssetSpec(key=AssetKey(["cloud_prod", "workspace", "metadata"])),
        AssetSpec(key=AssetKey(["cloud_prod", "workspace", "repo_metadata"])),
        AssetSpec(key=AssetKey(["cloud_prod", "workspace", "external_repo_metadata"])),
        *[
            AssetSpec(key=AssetKey(["cloud_prod", "workspace", dag_obj]))
            for dag_obj in DAGSTER_OBJECTS.values()
        ],
    ],
    partitions_def=org_partitions_def,
)
def workspace_data_json(context: AssetExecutionContext):
    s3_client = boto3.client("s3")
    bucket = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME, Prefix=f"workspace/{context.partition_key}"
    )

    for j, obj_info in enumerate(bucket["Contents"]):
        key = obj_info["Key"]
        output_directory = "processed"
        output_key_ending = "/".join(key.split("/")[1:])

        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        obj_dict = json.loads(obj["Body"].read().decode("utf-8"))

        # Pull the repo datas out of the object
        repository_datas = obj_dict.pop("repository_datas")

        metadata_output_key = "/".join([output_directory, "metadata", output_key_ending])
        s3_client.put_object(
            Bucket=BUCKET_NAME, Key=metadata_output_key, Body=json.dumps(obj_dict).encode("utf-8")
        )

        for repository_data in repository_datas:
            # Pull external repo datas out of the repository data
            external_repository_data = repository_data.pop("external_repository_data")

            repo_name = external_repository_data.get("repo_name", "__repository__")
            repo_metadata_output_key = "/".join(
                [output_directory, "repo_metadata", output_key_ending, repo_name]
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
                                    output_directory,
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
                [output_directory, "external_repo_metadata", output_key_ending, repo_name]
            )
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=external_repo_metadata_output_key,
                Body=json.dumps(external_repository_data).encode("utf-8"),
            )

    yield Output(None, output_name="cloud_prod__workspace__metadata")
    yield Output(None, output_name="cloud_prod__workspace__repo_metadata")
    yield Output(None, output_name="cloud_prod__workspace__external_repo_metadata")
    for dag_obj in DAGSTER_OBJECTS.values():
        yield Output(None, output_name=f"cloud_prod__workspace__{dag_obj}")
