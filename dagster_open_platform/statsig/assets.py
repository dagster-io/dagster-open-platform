from collections import defaultdict

import dagster as dg
import pandas as pd
from dagster_dbt import get_asset_key_for_model
from dagster_open_platform.dbt.assets import dbt_non_partitioned_models, dbt_partitioned_models
from dagster_open_platform.dbt.partitions import insights_partition
from dagster_open_platform.statsig.resources import DatadogMetricsResource, StatsigResource
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource

from statsig import StatsigEnvironmentTier
from statsig.statsig import StatsigEvent, StatsigUser


def _get_statsig_environment() -> str:
    env = get_environment()
    if env == "PROD":
        return StatsigEnvironmentTier.production.value
    return StatsigEnvironmentTier.development.value


def _convert_user_activity_record_to_statsig_event(record: dict, timestamp: int) -> StatsigEvent:
    del record["DS"]

    user = StatsigUser(
        user_id=record["USER_ID"],
        custom_ids={
            "stableID": record["ANONYMOUS_ID"],
            "organization_id": record["ORGANIZATION_ID"],
        },
    )
    return StatsigEvent(
        user=user,
        event_name="user_activity",
        metadata=record,
        statsigMetadata={
            "statsigTier": _get_statsig_environment(),
        },
        _time=timestamp,
    )


statsig_user_activity_daily = get_asset_key_for_model(
    [dbt_partitioned_models], "statsig_user_activity_daily"
)


@dg.asset(
    group_name="statsig",
    partitions_def=insights_partition,
    deps=[statsig_user_activity_daily],
    description="Daily events that contain pre-aggregated metrics based on user activity in segment.",
    tags={"dagster/kind/statsig": ""},
)
def user_activity_metrics(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
    statsig: StatsigResource,
):
    _, end_timestamp = (
        int(context.partition_time_window.start.timestamp()),
        int(context.partition_time_window.end.timestamp()),
    )

    database = get_database_for_environment()
    schema = get_schema_for_environment("METRICS_STATSIG")
    table_name = "STATSIG_USER_ACTIVITY_DAILY"
    qualified_name = ".".join([database, schema, table_name])

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM {qualified_name} WHERE DS = DATE('{context.partition_key_range.start}', 'YYYY-MM-DD-HH:MI')"
        )
        for batch in cur.fetch_pandas_batches():
            batch.fillna(0, inplace=True)
            result = statsig.write_events(
                [
                    _convert_user_activity_record_to_statsig_event(record, end_timestamp)
                    for record in batch.to_dict(orient="records")
                ]
            )
            context.log.info(f"Statsig write events response: {result.status_code} {result.text}")
        return dg.MaterializeResult(metadata={"dagster/row_count": cur.rowcount})


def _convert_org_performance_record_to_statsig_event(record: dict, timestamp: int) -> StatsigEvent:
    user = StatsigUser(
        custom_ids={
            "organization_id": record["ORGANIZATION_ID"],
        },
    )
    return StatsigEvent(
        user=user,
        event_name="org_performance",
        metadata=record,
        _time=timestamp,
    )


cloud_product_organizations = get_asset_key_for_model(
    [dbt_non_partitioned_models], "cloud_product_organizations"
)


@dg.asset(
    group_name="statsig",
    deps=[cloud_product_organizations],
    partitions_def=insights_partition,
    description="Daily events that contain pre-aggregated metrics based on org performance from datadog.",
    tags={"dagster/kind/statsig": ""},
)
def org_performance_metrics(
    context: dg.AssetExecutionContext,
    datadog: DatadogMetricsResource,
    statsig: StatsigResource,
    snowflake: SnowflakeResource,
):
    metrics = {
        "db_rows": "sum:dagster_cloud.request_cost.db_rowcount{*} by {organization}.rollup(sum, daily, 12am)",
        "db_duration": "sum:dagster_cloud.request_cost.db_duration_ms{*} by {organization}.rollup(sum, daily, 12am)",
        "request_size": "sum:dagster_cloud.request_cost.request_size{*} by {organization}.rollup(sum, daily, 12am)",
        "response_size": "sum:dagster_cloud.request_cost.response_size{*} by {organization}.rollup(sum, daily, 12am)",
    }
    start_timestamp, end_timestamp = (
        int(context.partition_time_window.start.timestamp()),
        int(context.partition_time_window.end.timestamp()),
    )

    all_metrics = defaultdict(dict)
    for name, query in metrics.items():
        metric_per_org = datadog.query_metrics_for_last_value(
            query,
            start_timestamp,
            end_timestamp,
        )
        for org, value in metric_per_org.items():
            all_metrics[org][name] = value

    # create pandas dataframe from all_metrics
    df = pd.DataFrame.from_dict(all_metrics, orient="index")
    df = df.reset_index().rename(columns={"index": "PUBLIC_ID"})
    df.fillna(0, inplace=True)

    database = get_database_for_environment()
    schema = get_schema_for_environment("MODEL_CLOUD_PRODUCT")
    table_name = "CLOUD_PRODUCT_ORGANIZATIONS"
    qualified_name = ".".join([database, schema, table_name])

    # query snowflake for organization_id
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT ORGANIZATION_ID, PUBLIC_ID FROM {qualified_name}")
        df = cur.fetch_pandas_all().merge(df, on="PUBLIC_ID")

    # write all the events to statsig
    result = statsig.write_events(
        [
            _convert_org_performance_record_to_statsig_event(record, end_timestamp)
            for record in df.to_dict(orient="records")
        ]
    )
    context.log.info(f"Statsig write events response: {result.status_code} {result.text}")
    return dg.MaterializeResult(metadata={"dagster/row_count": len(df)})
