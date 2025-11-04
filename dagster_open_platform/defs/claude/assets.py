from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, BackfillPolicy, asset
from dagster_open_platform.defs.claude.resources import AnthropicAdminResource
from dagster_open_platform.defs.claude.utils import (
    fetch_paginated_data,
    load_dataframe_to_snowflake,
)
from dagster_open_platform.defs.dbt.partitions import insights_partition
from dagster_snowflake import SnowflakeResource


@asset(
    group_name="anthropic_metrics",
    compute_kind="python",
    description="Fetches Anthropic usage data and loads into Snowflake",
    partitions_def=insights_partition,
    backfill_policy=BackfillPolicy.single_run(),
)
def anthropic_usage_report(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    anthropic_admin: AnthropicAdminResource,
) -> None:
    """Pulls usage data from Anthropic Admin API and loads into Snowflake.
    Fetches data at hourly granularity for the partition date.
    """
    admin_api_key = anthropic_admin.admin_api_key

    # Get the partition time window
    starting_at = context.partition_time_window.start
    ending_at = context.partition_time_window.end

    url = "https://api.anthropic.com/v1/organizations/usage_report/messages"
    headers = {"anthropic-version": "2023-06-01", "x-api-key": admin_api_key}
    params = {
        "starting_at": starting_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ending_at": ending_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bucket_width": "1h",
    }

    context.log.info(f"Fetching usage data from {starting_at} to {ending_at}")

    all_data, page_count = fetch_paginated_data(url, headers, params, context)

    context.log.info(f"Retrieved {len(all_data)} usage records across {page_count} pages")

    if not all_data:
        context.log.warning("No usage data returned")
        return

    # Log sample
    context.log.info(f"Sample record: {all_data[0]}")

    # Convert to DataFrame
    records = []
    for record in all_data:
        # Extract nested cache_creation fields
        cache_creation = record.get("cache_creation", {})
        ephemeral_1h_input_tokens = cache_creation.get("ephemeral_1h_input_tokens", 0)
        ephemeral_5m_input_tokens = cache_creation.get("ephemeral_5m_input_tokens", 0)

        # Extract nested server_tool_use fields
        server_tool_use = record.get("server_tool_use", {})
        web_search_requests = server_tool_use.get("web_search_requests", 0)

        records.append(
            {
                "UNCACHED_INPUT_TOKENS": record.get("uncached_input_tokens", 0),
                "CACHE_CREATION_EPHEMERAL_1H_INPUT_TOKENS": ephemeral_1h_input_tokens,
                "CACHE_CREATION_EPHEMERAL_5M_INPUT_TOKENS": ephemeral_5m_input_tokens,
                "CACHE_READ_INPUT_TOKENS": record.get("cache_read_input_tokens", 0),
                "OUTPUT_TOKENS": record.get("output_tokens", 0),
                "WEB_SEARCH_REQUESTS": web_search_requests,
                "API_KEY_ID": record.get("api_key_id"),
                "WORKSPACE_ID": record.get("workspace_id"),
                "MODEL": record.get("model"),
                "SERVICE_TIER": record.get("service_tier"),
                "CONTEXT_WINDOW": record.get("context_window"),
                "STARTING_AT": record.get("starting_at"),
                "ENDING_AT": record.get("ending_at"),
                "EXTRACTED_AT": datetime.now(timezone.utc).isoformat(),
            }
        )

    df = pd.DataFrame(records)

    # Load into Snowflake
    rows_inserted = load_dataframe_to_snowflake(
        df=df,
        snowflake=snowflake,
        context=context,
        create_table_sql="sql/create_usage_table.sql",
        temp_table_name="TEMP_ANTHROPIC_USAGE",
        target_table_name="ANTHROPIC_USAGE",
        delete_sql="sql/delete_usage_partition.sql",
        insert_sql="sql/merge_into_usage.sql",
    )

    context.log.info(
        f"Completed usage report: {len(all_data)} records, {rows_inserted} rows inserted"
    )


@asset(
    group_name="anthropic_metrics",
    compute_kind="python",
    description="Fetches Anthropic cost data and loads into Snowflake",
    partitions_def=insights_partition,
    backfill_policy=BackfillPolicy.single_run(),
)
def anthropic_cost_report(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    anthropic_admin: AnthropicAdminResource,
) -> None:
    """Pulls cost data from Anthropic Admin API and loads into Snowflake.
    Fetches data at daily granularity for the partition date.
    """
    admin_api_key = anthropic_admin.admin_api_key

    # Get the partition time window
    starting_at = context.partition_time_window.start
    ending_at = context.partition_time_window.end

    url = "https://api.anthropic.com/v1/organizations/cost_report"
    headers = {"anthropic-version": "2023-06-01", "x-api-key": admin_api_key}
    params = {
        "starting_at": starting_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ending_at": ending_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "group_by[]": ["workspace_id", "description"],
    }

    context.log.info(f"Fetching cost data from {starting_at} to {ending_at}")

    all_data, page_count = fetch_paginated_data(url, headers, params, context)

    context.log.info(f"Retrieved {len(all_data)} cost records across {page_count} pages")

    if not all_data:
        context.log.warning("No cost data returned")
        return

    # Calculate total cost
    total_cost_usd = sum(float(r.get("amount", 0)) for r in all_data)

    # Log sample and total
    context.log.info(f"Total cost over period: ${total_cost_usd:.2f}")

    # Convert to DataFrame
    records = []
    for record in all_data:
        records.append(
            {
                "CURRENCY": record.get("currency"),
                "AMOUNT": float(record.get("amount", 0)),
                "WORKSPACE_ID": record.get("workspace_id"),
                "DESCRIPTION": record.get("description"),
                "COST_TYPE": record.get("cost_type"),
                "CONTEXT_WINDOW": record.get("context_window"),
                "MODEL": record.get("model"),
                "SERVICE_TIER": record.get("service_tier"),
                "TOKEN_TYPE": record.get("token_type"),
                "STARTING_AT": record.get("starting_at"),
                "ENDING_AT": record.get("ending_at"),
                "EXTRACTED_AT": datetime.now(timezone.utc).isoformat(),
            }
        )

    df = pd.DataFrame(records)

    # Load into Snowflake
    rows_inserted = load_dataframe_to_snowflake(
        df=df,
        snowflake=snowflake,
        context=context,
        create_table_sql="sql/create_cost_table.sql",
        temp_table_name="TEMP_ANTHROPIC_COSTS",
        target_table_name="ANTHROPIC_COSTS",
        delete_sql="sql/delete_cost_partition.sql",
        insert_sql="sql/merge_into_cost.sql",
    )

    context.log.info(
        f"Completed cost report: {len(all_data)} records, {rows_inserted} rows inserted, "
        f"total cost: ${total_cost_usd:.2f}"
    )
