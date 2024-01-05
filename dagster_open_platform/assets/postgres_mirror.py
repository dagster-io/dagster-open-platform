import re

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    StaticPartitionsDefinition,
    asset,
)

from ..resources.sling_resource import (
    SlingMode,
    SlingResource,
    build_sync_postgres_to_snowflake_asset,
)

# Split into 4 static partitions in order to avoid long-running queries that are canceled by
# the read replica when it syncs w/ the primary
REPO_LOCATION_DATA_NUM_CHUNKS = 5

SOURCE_STREAM = (
    "select id, organization_id, deployment_id, create_timestamp, update_timestamp,"
    " jsonb_path_query_array("
    "load_repositories_response::jsonb,"
    " '$.repository_datas[0].external_repository_data.external_asset_graph_data[*].asset_key'"
    ") as asset_keys,"
    " jsonb_path_query_array("
    "load_repositories_response::jsonb,"
    " '$.repository_datas[0].external_repository_data.external_asset_graph_data[*].group_name'"
    ") as group_names from repository_locations_data"
    f" where id % {REPO_LOCATION_DATA_NUM_CHUNKS} = "
)

REPO_LOCATION_DATA_CHUNKED = StaticPartitionsDefinition(
    [str(x) for x in range(REPO_LOCATION_DATA_NUM_CHUNKS)]
)


def define_sync_repo_location_data_asset(
    database: str,
    sling_resource_key: str,
) -> AssetsDefinition:
    @asset(
        key=AssetKey([database, "postgres_mirror", "repository_locations_data"]),
        partitions_def=REPO_LOCATION_DATA_CHUNKED,
        required_resource_keys={sling_resource_key},
    )
    def sync_repo_location_data(
        context: AssetExecutionContext,
    ) -> None:
        sling: SlingResource = getattr(context.resources, sling_resource_key)

        for partition_key in REPO_LOCATION_DATA_CHUNKED.get_partition_keys_in_range(
            context.partition_key_range
        ):
            context.log.info("Syncing partition: %s", partition_key)
            for stdout_line in sling.sync_postgres_to_snowflake(
                source_table=f"{SOURCE_STREAM} {partition_key}",
                dest_table="postgres_mirror.repository_locations_data",
                primary_key=["id"],
                mode=SlingMode.INCREMENTAL,
            ):
                print(stdout_line)
                cleaned_line = re.sub(r"\[[0-9;]+[a-zA-Z]", " ", stdout_line)
                trimmed_line = cleaned_line[cleaned_line.find("INF") + 6 :].strip()
                context.log.info(trimmed_line)

    return sync_repo_location_data


sync_repo_location_data = define_sync_repo_location_data_asset(
    "purina", "cloud_prod_read_replica_sling"
)

TABLES_TO_SYNC_FULL_REPLICA_REPORTING = ["reporting_deployment_settings"]


postgres_replica_reporting_assets = [
    build_sync_postgres_to_snowflake_asset(
        key=AssetKey(["purina", "postgres_mirror", table_name]),
        sling_resource_key="cloud_prod_reporting_sling",
        source_table=table_name,
        dest_table=f"postgres_mirror.{table_name}",
        mode=SlingMode.FULL_REFRESH,
    )
    for table_name in TABLES_TO_SYNC_FULL_REPLICA_REPORTING
]
