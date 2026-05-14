from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, AutomationCondition, BackfillPolicy, asset
from dagster_snowflake import SnowflakeResource

from dagster_open_platform.defs.claude.partitions import claude_daily_partition
from dagster_open_platform.defs.claude.resources import AnthropicAdminResource
from dagster_open_platform.defs.claude.utils import (
    fetch_claude_code_data,
    fetch_paginated_data,
    load_dataframe_to_snowflake,
)

daily_cron_tick_passed = (
    AutomationCondition.cron_tick_passed("0 0 * * *")
    & ~AutomationCondition.in_progress()
    & AutomationCondition.in_latest_time_window()
)


@asset(
    group_name="anthropic_metrics",
    compute_kind="python",
    description="Fetches Anthropic usage data and loads into Snowflake",
    partitions_def=claude_daily_partition,
    backfill_policy=BackfillPolicy.single_run(),
    automation_condition=daily_cron_tick_passed,
    tags={"dagster/kind/claude": ""},
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
        "group_by[]": [
            "api_key_id",
            "workspace_id",
            "model",
            "service_tier",
            "context_window",
            "account_id",
            "inference_geo",
            "service_account_id",
        ],
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
                "ACCOUNT_ID": record.get("account_id"),
                "INFERENCE_GEO": record.get("inference_geo"),
                "SERVICE_ACCOUNT_ID": record.get("service_account_id"),
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
    partitions_def=claude_daily_partition,
    backfill_policy=BackfillPolicy.single_run(),
    automation_condition=daily_cron_tick_passed,
    tags={"dagster/kind/claude": ""},
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
                "INFERENCE_GEO": record.get("inference_geo"),
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


@asset(
    group_name="anthropic_metrics",
    compute_kind="python",
    description="Fetches Claude Code per-user analytics and loads into Snowflake",
    partitions_def=claude_daily_partition,
    backfill_policy=BackfillPolicy.single_run(),
    automation_condition=daily_cron_tick_passed,
    tags={"dagster/kind/claude": ""},
)
def anthropic_claude_code_report(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    anthropic_admin: AnthropicAdminResource,
) -> None:
    """Pulls Claude Code analytics from Anthropic Admin API and loads into Snowflake.

    Returns daily user-level metrics including sessions, lines of code, commits,
    pull requests, tool acceptance rates, and per-model token/cost breakdowns.
    Flattened to one row per user per model per day.
    """
    admin_api_key = anthropic_admin.admin_api_key

    starting_at = context.partition_time_window.start

    url = "https://api.anthropic.com/v1/organizations/usage_report/claude_code"
    headers = {"anthropic-version": "2023-06-01", "x-api-key": admin_api_key}
    params = {
        "starting_at": starting_at.strftime("%Y-%m-%d"),
        "limit": 1000,
    }

    context.log.info(f"Fetching Claude Code analytics for {starting_at.date()}")

    all_data, page_count = fetch_claude_code_data(url, headers, params, context)

    context.log.info(f"Retrieved {len(all_data)} user records across {page_count} pages")

    if not all_data:
        context.log.warning("No Claude Code analytics data returned")
        return

    extracted_at = datetime.now(timezone.utc).isoformat()
    records = []
    for record in all_data:
        actor = record.get("actor", {})
        actor_type = actor.get("type")
        actor_email = actor.get("email_address") if actor_type == "user_actor" else None
        actor_api_key_name = actor.get("api_key_name") if actor_type == "api_actor" else None

        core = record.get("core_metrics", {})
        loc = core.get("lines_of_code", {})
        tools = record.get("tool_actions", {})

        base = {
            "DATE": record.get("date"),
            "ACTOR_TYPE": actor_type,
            "ACTOR_EMAIL": actor_email,
            "ACTOR_API_KEY_NAME": actor_api_key_name,
            "ORGANIZATION_ID": record.get("organization_id"),
            "CUSTOMER_TYPE": record.get("customer_type"),
            "TERMINAL_TYPE": record.get("terminal_type"),
            "NUM_SESSIONS": core.get("num_sessions", 0),
            "LINES_ADDED": loc.get("added", 0),
            "LINES_REMOVED": loc.get("removed", 0),
            "COMMITS_BY_CLAUDE_CODE": core.get("commits_by_claude_code", 0),
            "PULL_REQUESTS_BY_CLAUDE_CODE": core.get("pull_requests_by_claude_code", 0),
            "EDIT_TOOL_ACCEPTED": tools.get("edit_tool", {}).get("accepted", 0),
            "EDIT_TOOL_REJECTED": tools.get("edit_tool", {}).get("rejected", 0),
            "MULTI_EDIT_TOOL_ACCEPTED": tools.get("multi_edit_tool", {}).get("accepted", 0),
            "MULTI_EDIT_TOOL_REJECTED": tools.get("multi_edit_tool", {}).get("rejected", 0),
            "WRITE_TOOL_ACCEPTED": tools.get("write_tool", {}).get("accepted", 0),
            "WRITE_TOOL_REJECTED": tools.get("write_tool", {}).get("rejected", 0),
            "NOTEBOOK_EDIT_TOOL_ACCEPTED": tools.get("notebook_edit_tool", {}).get("accepted", 0),
            "NOTEBOOK_EDIT_TOOL_REJECTED": tools.get("notebook_edit_tool", {}).get("rejected", 0),
            "EXTRACTED_AT": extracted_at,
        }

        model_breakdown = record.get("model_breakdown", [])
        if model_breakdown:
            for model_entry in model_breakdown:
                tokens = model_entry.get("tokens", {})
                cost = model_entry.get("estimated_cost", {})
                records.append(
                    {
                        **base,
                        "MODEL": model_entry.get("model"),
                        "INPUT_TOKENS": tokens.get("input", 0),
                        "OUTPUT_TOKENS": tokens.get("output", 0),
                        "CACHE_READ_TOKENS": tokens.get("cache_read", 0),
                        "CACHE_CREATION_TOKENS": tokens.get("cache_creation", 0),
                        "ESTIMATED_COST_CENTS": cost.get("amount"),
                        "ESTIMATED_COST_CURRENCY": cost.get("currency"),
                    }
                )
        else:
            records.append(
                {
                    **base,
                    "MODEL": None,
                    "INPUT_TOKENS": None,
                    "OUTPUT_TOKENS": None,
                    "CACHE_READ_TOKENS": None,
                    "CACHE_CREATION_TOKENS": None,
                    "ESTIMATED_COST_CENTS": None,
                    "ESTIMATED_COST_CURRENCY": None,
                }
            )

    df = pd.DataFrame(records)

    rows_inserted = load_dataframe_to_snowflake(
        df=df,
        snowflake=snowflake,
        context=context,
        create_table_sql="sql/create_claude_code_table.sql",
        temp_table_name="TEMP_ANTHROPIC_CLAUDE_CODE",
        target_table_name="ANTHROPIC_CLAUDE_CODE",
        delete_sql="sql/delete_claude_code_partition.sql",
        insert_sql="sql/merge_into_claude_code.sql",
    )

    context.log.info(
        f"Completed Claude Code report: {len(all_data)} user records, {rows_inserted} rows inserted"
    )
