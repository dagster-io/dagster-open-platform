import re
from datetime import datetime, timedelta

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    Config,
    StaticPartitionsDefinition,
    asset,
)
from pydantic import Field

from ..resources.sling_resource import (
    SlingMode,
    SlingResource,
)

# Split into many static partitions in order to avoid long-running queries that are canceled by
# the read replica when it syncs w/ the primary - smaller queries are less likely to be canceled
REPO_LOCATION_DATA_NUM_CHUNKS = 5

SOURCE_STREAM = (
    "select id, organization_id, deployment_id, create_timestamp, timestamp as update_timestamp,"
    " jsonb_path_query_array("
    "load_repositories_response::jsonb,"
    " '$.repository_datas[0].external_repository_data.external_asset_graph_data[*].asset_key'"
    ") as asset_keys,"
    " jsonb_path_query_array("
    "load_repositories_response::jsonb,"
    " '$.repository_datas[0].external_repository_data.external_asset_graph_data[*].group_name'"
    ") as group_names from repository_locations_data"
    " where timestamp > '{lookback_timestamp}'"
    f" and id % {REPO_LOCATION_DATA_NUM_CHUNKS} = "
)

REPO_LOCATION_DATA_CHUNKED = StaticPartitionsDefinition(
    [str(x) for x in range(REPO_LOCATION_DATA_NUM_CHUNKS)]
)


class SyncRepoLocationDataConfig(Config):
    lookback_days: int = Field(
        default=1, description="Number of days of repo location updates to sync."
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
        config: SyncRepoLocationDataConfig,
    ) -> None:
        sling: SlingResource = getattr(context.resources, sling_resource_key)

        for partition_key in REPO_LOCATION_DATA_CHUNKED.get_partition_keys_in_range(
            context.partition_key_range
        ):
            # we sync data from 1 day ago to give us a time buffer
            lookback_timestamp = (datetime.now() - timedelta(days=config.lookback_days)).strftime(
                "%Y-%m-%d"
            )

            context.log.info("Syncing partition: %s", partition_key)
            for stdout_line in sling.sync_postgres_to_snowflake(
                source_table=f"{SOURCE_STREAM.format(lookback_timestamp=lookback_timestamp)} {partition_key}",
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
