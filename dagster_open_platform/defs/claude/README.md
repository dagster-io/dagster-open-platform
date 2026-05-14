# Anthropic Usage & Cost Metrics

This module contains Dagster assets for ingesting Anthropic usage and cost data into Snowflake.

## Assets

### `anthropic_usage_report`
Fetches hourly token usage from the Anthropic Admin API, grouped by all available dimensions.

- **Endpoint**: `GET /v1/organizations/usage_report/messages`
- **Table**: `ANTHROPIC.RAW.ANTHROPIC_USAGE`
- **Granularity**: Hourly buckets, one row per unique combination of api_key × workspace × model × service_tier × context_window × account × inference_geo × service_account
- **Schedule**: Daily (automation condition: cron tick at midnight UTC)

### `anthropic_cost_report`
Fetches daily dollar costs from the Anthropic Admin API grouped by workspace and description.

- **Endpoint**: `GET /v1/organizations/cost_report`
- **Table**: `ANTHROPIC.RAW.ANTHROPIC_COSTS`
- **Granularity**: Daily buckets, one row per workspace × description line item
- **Schedule**: Daily (automation condition: cron tick at midnight UTC)

### `anthropic_claude_code_report`
Fetches daily per-user Claude Code analytics including productivity metrics, tool acceptance rates, and per-model token/cost breakdowns.

- **Endpoint**: `GET /v1/organizations/usage_report/claude_code`
- **Table**: `ANTHROPIC.RAW.ANTHROPIC_CLAUDE_CODE`
- **Granularity**: Daily, one row per user × model
- **Schedule**: Daily (automation condition: cron tick at midnight UTC)

## Resources

### `AnthropicAdminResource`
Manages authentication for the Anthropic Admin API using an Admin API key (separate from the regular API key, starts with `sk-ant-admin...`).

**Required environment variable:**
```bash
ANTHROPIC_ADMIN_API_KEY=your_admin_api_key_here
```

## Snowflake Tables

### ANTHROPIC_USAGE
| Column | Type | Description |
|--------|------|-------------|
| uncached_input_tokens | NUMBER | Uncached input tokens processed |
| cache_creation_ephemeral_1h_input_tokens | NUMBER | Tokens for 1-hour cache creation |
| cache_creation_ephemeral_5m_input_tokens | NUMBER | Tokens for 5-minute cache creation |
| cache_read_input_tokens | NUMBER | Input tokens read from cache |
| output_tokens | NUMBER | Output tokens generated |
| web_search_requests | NUMBER | Server-side web search calls |
| api_key_id | VARCHAR | API key used (null for Console usage) |
| workspace_id | VARCHAR | Workspace ID (null for default workspace) |
| model | VARCHAR | Model used |
| service_tier | VARCHAR | standard, batch, priority, flex, etc. |
| context_window | VARCHAR | 0-200k or 200k-1M |
| account_id | VARCHAR | OAuth user account ID (null for API key requests) |
| inference_geo | VARCHAR | us, global, or not_available |
| service_account_id | VARCHAR | OIDC-federated service account ID |
| starting_at | TIMESTAMP_NTZ | Start of hourly bucket |
| ending_at | TIMESTAMP_NTZ | End of hourly bucket |
| extracted_at | TIMESTAMP_NTZ | Extraction timestamp |

### ANTHROPIC_COSTS
| Column | Type | Description |
|--------|------|-------------|
| currency | VARCHAR | Always USD |
| amount | FLOAT | Cost in cents |
| workspace_id | VARCHAR | Workspace ID |
| description | VARCHAR | Line item description |
| cost_type | VARCHAR | tokens, web_search, code_execution, session_usage |
| context_window | VARCHAR | 0-200k or 200k-1M |
| model | VARCHAR | Model (null for non-token costs) |
| service_tier | VARCHAR | standard or batch |
| token_type | VARCHAR | uncached_input_tokens, output_tokens, cache_*, etc. |
| inference_geo | VARCHAR | us, global, or not_available |
| starting_at | TIMESTAMP_NTZ | Start of daily bucket |
| ending_at | TIMESTAMP_NTZ | End of daily bucket |
| extracted_at | TIMESTAMP_NTZ | Extraction timestamp |

### ANTHROPIC_CLAUDE_CODE
| Column | Type | Description |
|--------|------|-------------|
| date | TIMESTAMP_NTZ | Date of activity (UTC) |
| actor_type | VARCHAR | user_actor (OAuth) or api_actor (API key) |
| actor_email | VARCHAR | User email — populated for user_actor |
| actor_api_key_name | VARCHAR | API key name — populated for api_actor |
| organization_id | VARCHAR | Organization UUID |
| customer_type | VARCHAR | api or subscription |
| terminal_type | VARCHAR | vscode, iTerm.app, tmux, etc. |
| num_sessions | NUMBER | Distinct Claude Code sessions |
| lines_added | NUMBER | Lines of code added by Claude Code |
| lines_removed | NUMBER | Lines of code removed by Claude Code |
| commits_by_claude_code | NUMBER | Git commits via Claude Code |
| pull_requests_by_claude_code | NUMBER | PRs via Claude Code |
| edit_tool_accepted/rejected | NUMBER | Edit tool proposal outcomes |
| multi_edit_tool_accepted/rejected | NUMBER | MultiEdit tool proposal outcomes |
| write_tool_accepted/rejected | NUMBER | Write tool proposal outcomes |
| notebook_edit_tool_accepted/rejected | NUMBER | NotebookEdit tool proposal outcomes |
| model | VARCHAR | Claude model (null if no breakdown) |
| input_tokens | NUMBER | Input tokens for this model |
| output_tokens | NUMBER | Output tokens for this model |
| cache_read_tokens | NUMBER | Cache read tokens for this model |
| cache_creation_tokens | NUMBER | Cache creation tokens for this model |
| estimated_cost_cents | NUMBER | Estimated cost in cents for this model |
| estimated_cost_currency | VARCHAR | Always USD |
| extracted_at | TIMESTAMP_NTZ | Extraction timestamp |

> **Note:** Productivity metrics (num_sessions, lines_added/removed, commits, PRs, tool actions) are at the user-day grain. Do not sum them across models for the same user on the same day.

## Backfill

All three assets use `BackfillPolicy.single_run()` and are partitioned daily from `2025-01-01`. To backfill, select all partitions in the Dagster UI and launch a backfill.

> **Important:** The usage report previously ran without `group_by` parameters, meaning historical data before this fix has null values for all dimension columns. A full backfill from `2025-01-01` is recommended to recover correct data.
