import json
import os
from datetime import datetime

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    DailyPartitionsDefinition,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    Output,
    asset,
    multi_asset,
)
from dagster_aws.s3 import S3Resource
from dagster_open_platform.aws.constants import (
    ACCOUNT_NAME,
    BASE_S3_LOCATION,
    BUCKET_NAME,
    COPY_DATA_QUERY,
    CREATE_TABLE_QUERY,
    DAGSTER_METADATA_OBJECTS,
    DAGSTER_OBJECTS,
    DELETE_PARTITION_QUERY,
    INPUT_PREFIX,
    OUTPUT_PREFIX,
)
from dagster_open_platform.aws.sensors import org_partitions_def
from dagster_open_platform.aws.utils import S3Mailman
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource

# metdata, repo_metadata, external_repo_metadata
dagster_metadata_asset_specs = [
    AssetSpec(
        key=AssetKey([BUCKET_NAME, "staging", dag_metadata_obj]),
        metadata={
            "aws_account": ACCOUNT_NAME,
            "s3_location": f"s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/{dag_metadata_obj}",
        },
    )
    for dag_metadata_obj in DAGSTER_METADATA_OBJECTS
]

# assets, asset checks, sensors, etc.
dagster_objet_asset_specs = [
    AssetSpec(
        key=AssetKey([BUCKET_NAME, "staging", dag_obj]),
        metadata={
            "aws_account": ACCOUNT_NAME,
            "s3_location": f"s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/{dag_obj}",
        },
    )
    for dag_obj in DAGSTER_OBJECTS.values()
]


@multi_asset(
    group_name="aws",
    specs=dagster_metadata_asset_specs + dagster_objet_asset_specs,
    description="Assets for AWS workspace replication data",
    partitions_def=MultiPartitionsDefinition(
        {
            "date": DailyPartitionsDefinition(start_date="2024-08-14"),
            "organization": org_partitions_def,
        }
    ),
)
def workspace_data_json(context: AssetExecutionContext, s3_resource: S3Resource):
    s3_client = s3_resource.get_client()
    date, org = context.partition_key.split("|")
    s3_mailman = S3Mailman(
        bucket_name=BUCKET_NAME,
        input_prefix=f"{INPUT_PREFIX}/{date}/{org}/",
        output_prefix=OUTPUT_PREFIX,
        s3_client=s3_client,
    )

    bucket_contents = s3_mailman.get_contents()

    object_count = {}
    for obj_info in bucket_contents:
        key = obj_info.get("Key", "")
        output_key_ending = "/".join(key.split("/")[2:])

        obj_dict = json.loads(s3_mailman.get_body(key, decode="utf-8"))

        # Pull the repo datas out of the object
        repository_datas = obj_dict.pop("repository_datas")

        metadata_output_key = os.path.join("metadata", output_key_ending)
        s3_mailman.send(json.dumps(obj_dict), metadata_output_key, encode="utf-8")

        for repository_data in repository_datas:
            # Pull external repo datas out of the repository data
            external_repository_data = repository_data.pop("external_repository_data")

            repo_name = external_repository_data.get("repo_name", "__repository__")
            repo_metadata_output_key = os.path.join("repo_metadata", output_key_ending, repo_name)
            s3_mailman.send(json.dumps(repository_data), repo_metadata_output_key, encode="utf-8")

            for dagster_object_key, dagster_object_name in DAGSTER_OBJECTS.items():
                dagster_object = external_repository_data.pop(dagster_object_key, []) or []
                if dagster_object_name not in object_count:
                    object_count[dagster_object_name] = len(dagster_object)
                else:
                    object_count[dagster_object_name] += len(dagster_object)
                dagster_object_output_base_path = os.path.join(
                    dagster_object_name, output_key_ending, repo_name
                )
                s3_mailman.send_all(
                    dagster_object,
                    dagster_object_output_base_path,
                    encode="utf-8",
                    preprocess=lambda x: json.dumps(x),
                )

            external_repo_metadata_output_key = os.path.join(
                "external_repo_metadata", output_key_ending, repo_name
            )
            s3_mailman.send(
                json.dumps(external_repository_data),
                external_repo_metadata_output_key,
                encode="utf-8",
            )

    for metadata_obj in DAGSTER_METADATA_OBJECTS:
        yield Output(None, output_name=f"{BUCKET_NAME.replace('-', '_')}__staging__{metadata_obj}")
    for dag_obj in DAGSTER_OBJECTS.values():
        yield Output(
            None,
            output_name=f"{BUCKET_NAME.replace('-', '_')}__staging__{dag_obj}",
            metadata={"count": object_count.get(dag_obj, 0)},
        )


aws_monthly_partition = MonthlyPartitionsDefinition(start_date="2020-12-01", end_offset=1)

materialize_on_cron_policy = AutoMaterializePolicy.eager().with_rules(
    AutoMaterializeRule.materialize_on_cron("0 */4 * * *"),
)


@asset(partitions_def=aws_monthly_partition, auto_materialize_policy=materialize_on_cron_policy)
def aws_cost_report(context: AssetExecutionContext, snowflake_aws: SnowflakeResource):
    """AWS updates the monthly cost report once an hour, overwriting the existing
    files for the current month.

    """
    database = get_database_for_environment()
    schema = get_schema_for_environment("FINANCE")
    table_name = "aws_cost_report"
    qualified_name = ".".join([database, schema, table_name])

    partition = context.partition_key
    partition_no_day = datetime.strptime(partition, "%Y-%m-%d").strftime("%Y-%m")

    create_table = CREATE_TABLE_QUERY.format(
        QUALIFIED_TABLE_NAME=qualified_name, BASE_S3_LOCATION=BASE_S3_LOCATION
    ).format(PARTITION_MONTH=partition_no_day)
    context.log.info(f"SQL debug {create_table}")

    copy_table = COPY_DATA_QUERY.format(
        QUALIFIED_TABLE_NAME=qualified_name, BASE_S3_LOCATION=BASE_S3_LOCATION
    ).format(PARTITION_MONTH=partition_no_day)
    context.log.info(f"SQL debug {copy_table}")

    delete_partition = DELETE_PARTITION_QUERY.format(
        QUALIFIED_TABLE_NAME=qualified_name,
        PARTITION_MONTH=partition,
    )
    context.log.info(f"SQL debug {delete_partition}")

    with snowflake_aws.get_connection() as conn:
        conn.autocommit(False)

        cur = conn.cursor()
        try:
            cur.execute(create_table)
            context.log.info(f"Table {qualified_name} successfully created")
            cur.execute(delete_partition)
            context.log.info(f"Table {qualified_name} partition {partition} successfully deleted")
            cur.execute(copy_table)
            context.log.info(f"Data successfully copied into {qualified_name}")
            cur.execute(f"SELECT COUNT(*) FROM {qualified_name}")
            rows = cur.fetchone()[0]  # type: ignore
            context.log.info(f"{rows} rows inserted into {qualified_name}")
        except Exception as e:
            conn.rollback()
            context.log.error(f"Error loading data into {qualified_name}")
            raise e

    return MaterializeResult(
        metadata={
            "snowflake_table": qualified_name,
            "rows_inserted": rows,
        }
    )
