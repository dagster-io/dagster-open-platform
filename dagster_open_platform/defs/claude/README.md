# Anthropic Usage & Cost Metrics

This module contains Dagster assets for ingesting Anthropic usage and cost data into Snowflake.

## Structure

This module follows the standard structure pattern used across the project:

- **`assets.py`** - Asset definitions for usage and cost reports
- **`resources.py`** - Custom `AnthropicAdminResource` for Admin API access
- **`schedules.py`** - Daily schedule definitions
- **`definitions.py`** - Wires together assets, schedules, and resources

## Assets

### `anthropic_usage_report`
Fetches usage data from the Anthropic Admin API and loads it into Snowflake at hourly granularity.

- **Table**: `raw.anthropic_usage`
- **Schedule**: Daily at 1 AM UTC
- **Lookback**: 3 days by default (to catch late-arriving data)

### `anthropic_cost_report`
Fetches cost data from the Anthropic Admin API and loads it into Snowflake at daily granularity.

- **Table**: `raw.anthropic_costs`
- **Schedule**: Daily at 2 AM UTC
- **Lookback**: 3 days by default (to catch late-arriving data)

## Resources

### `AnthropicAdminResource`
A custom Dagster `ConfigurableResource` that manages authentication for the Anthropic Admin API.

**Configuration:**
- `admin_api_key` - The Anthropic Admin API key (separate from the regular API key)

## Setup

### Environment Variables

The following environment variable is required:

```bash
ANTHROPIC_ADMIN_API_KEY=your_admin_api_key_here
```

This is configured via the `AnthropicAdminResource` in `definitions.py` using `EnvVar`.

### Optional Environment Variables

For initial backfill or custom lookback periods:

```bash
# For usage data (default: 3)
ANTHROPIC_USAGE_LOOKBACK_DAYS=90

# For cost data (default: 3)
ANTHROPIC_COST_LOOKBACK_DAYS=90
```

## Snowflake Tables

### raw.anthropic_usage

```sql
CREATE TABLE raw.anthropic_usage (
    timestamp TIMESTAMP_NTZ,
    model VARCHAR(100),
    service_tier VARCHAR(50),
    context_window VARCHAR(50),
    workspace_id VARCHAR(100),
    api_key_id VARCHAR(100),
    input_tokens NUMBER,
    output_tokens NUMBER,
    cache_creation_input_tokens NUMBER,
    cache_read_input_tokens NUMBER,
    requests NUMBER,
    extracted_at TIMESTAMP_NTZ,
    PRIMARY KEY (timestamp, model, workspace_id, api_key_id)
)
```

### raw.anthropic_costs

```sql
CREATE TABLE raw.anthropic_costs (
    timestamp TIMESTAMP_NTZ,
    workspace_id VARCHAR(100),
    description VARCHAR(500),
    cost_usd_cents NUMBER,
    cost_usd FLOAT,
    extracted_at TIMESTAMP_NTZ,
    PRIMARY KEY (timestamp, workspace_id, description)
)
```

## Usage

### Running Assets Manually

In the Dagster UI, navigate to the Assets page and materialize:
- `anthropic_usage_report`
- `anthropic_cost_report`

### Scheduled Runs

Both assets run automatically on their daily schedules:
- Usage report: 1 AM UTC
- Cost report: 2 AM UTC

### Initial Backfill

To backfill historical data, set the appropriate lookback environment variable and manually materialize the assets:

```bash
export ANTHROPIC_USAGE_LOOKBACK_DAYS=90
export ANTHROPIC_COST_LOOKBACK_DAYS=90
# Then materialize the assets in Dagster UI
```

## Data Merge Strategy

Both assets use a MERGE strategy to:
- **UPDATE** existing records when the primary key matches
- **INSERT** new records when no match is found

This ensures that late-arriving or updated data is properly reflected in the tables.

