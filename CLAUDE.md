# Dagster Open Platform (DOP)

This document provides comprehensive guidance for working with the Dagster Open Platform codebase.

## Overview

**Dagster Open Platform** is Dagster Labs' internal data platform containing real production assets used for analytics and operations. This project serves as both:
1. A production data platform for internal Dagster Labs operations
2. An educational reference demonstrating best practices for building data platforms with Dagster

**Key Features:**
- Real-world examples of declarative automation
- Freshness checks and asset health monitoring
- Metadata and data quality patterns
- Multi-source data integration (Fivetran, Segment, Snowflake, dbt, Sling, etc.)
- Component-based architecture for reusable patterns
- Git-integrated code references

## Repository Structure

```
dagster-open-platform/
├── dagster_open_platform/          # Main package
│   ├── definitions.py              # Root definitions file (entry point)
│   ├── defs/                       # Individual pipeline definitions
│   │   ├── aws/                    # AWS-related pipelines
│   │   ├── claude/                 # Claude AI-related pipelines
│   │   ├── dbt/                    # dbt transformation pipelines
│   │   ├── fivetran/               # Fivetran sync definitions
│   │   ├── pypi/                   # PyPI download analytics
│   │   ├── segment/                # Segment event data
│   │   ├── snowflake/              # Snowflake-native assets
│   │   ├── sling/                  # Sling data ingestion
│   │   ├── stripe/                 # Stripe billing data
│   │   └── ...                     # Many more integrations
│   ├── lib/                        # Shared library code
│   │   ├── executable_component.py # Base component classes
│   │   ├── schedule.py             # Schedule utilities
│   │   ├── dbt/                    # dbt integration helpers
│   │   ├── fivetran/               # Fivetran component implementations
│   │   ├── snowflake/              # Snowflake component helpers
│   │   ├── sling/                  # Sling utilities
│   │   └── ...                     # Other integration libraries
│   └── utils/                      # Utility functions
│       ├── environment_helpers.py  # Environment-specific config
│       ├── source_code.py          # Git code reference linking
│       └── github_gql_queries.py   # GitHub GraphQL queries
├── dagster_open_platform_dbt/      # dbt project directory
├── dagster_open_platform_tests/    # Test suite
├── dagster_open_platform_eu/       # EU-specific deployment config
├── pyproject.toml                  # Package configuration
└── README.md                       # Public-facing documentation
```

## Core Concepts

### 1. Component-Based Architecture

DOP uses **Dagster components** extensively for reusable patterns. Components are defined in `lib/` and instantiated via YAML configuration in `defs/`.

**Component Structure:**
```
defs/fivetran/
├── components/
│   └── component.yaml          # Component configuration
└── py/
    ├── checks.py               # Asset checks
    └── definitions.py          # Python-based definitions
```

**Example Component (component.yaml):**
```yaml
type: dagster_open_platform.lib.FivetranComponent

attributes:
  workspace:
    account_id: "{{ env('FIVETRAN_ACCOUNT_ID') }}"
    api_key: "{{ env('FIVETRAN_API_KEY') }}"
  translation:
    key: "fivetran/{{ props.table }}"
    automation_condition: "{{ hourly_if_not_in_progress }}"
```

**Component Implementation (`lib/`):**
- Custom components extend base classes from `lib/executable_component.py`
- Components handle integration-specific logic (Fivetran, Segment, Snowflake, etc.)
- Registered in `pyproject.toml` under `[tool.dg.project]` `registry_modules`

### 2. Asset Definitions

Assets can be defined in two ways:

**A. Python-based Assets (`py/` directories):**
```python
@dg.asset(
    tags={"dagster/kind/snowflake": ""},
    key=["purina", "oss_analytics", "dagster_pypi_downloads"],
    group_name="oss_analytics",
    partitions_def=oss_analytics_daily_partition,
    automation_condition=dg.AutomationCondition.eager(),
    check_specs=[
        dg.AssetCheckSpec("non_empty_check", asset=asset_key),
    ],
)
def my_asset(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """Asset description with business context."""
    # Implementation here
    return dg.MaterializeResult(metadata={...})
```

**B. Component-based Assets:**
- Defined via YAML configuration in `components/component.yaml`
- Component classes in `lib/` handle instantiation and execution
- Enables declarative configuration for common patterns

### 3. Automation & Scheduling

**Declarative Automation:**
```python
automation_condition=dg.AutomationCondition.eager()  # Run as soon as deps are ready
automation_condition=dg.AutomationCondition.on_cron("0 0 * * *")  # Daily at midnight
```

**Global Freshness Policy:**
```python
# In definitions.py
global_freshness_policy = InternalFreshnessPolicy.time_window(fail_window=timedelta(hours=36))
```

**Schedule Patterns:**
- Schedules defined in `defs/schedules/` or per-integration directories
- Common patterns: cron-based, on-demand, and freshness-driven

### 4. Asset Checks & Data Quality

**Asset Check Patterns:**
```python
check_specs=[
    dg.AssetCheckSpec("non_empty_check", asset=asset_key),
    dg.AssetCheckSpec("freshness_check", asset=asset_key),
    dg.AssetCheckSpec("schema_validation", asset=asset_key),
]

# Check implementation
@dg.asset_check(asset=asset_key, name="non_empty_check")
def check_non_empty(context, snowflake):
    result = snowflake.execute_query(f"SELECT COUNT(*) FROM {table}")
    passed = result[0][0] > 0
    return dg.AssetCheckResult(passed=passed, metadata={...})
```

### 5. Metadata & Observability

**Adding Metadata:**
```python
return dg.MaterializeResult(
    metadata={
        "num_rows": dg.MetadataValue.int(row_count),
        "last_updated": dg.MetadataValue.timestamp(current_time),
        "query": dg.MetadataValue.md(query_text),
        "preview": dg.MetadataValue.table(df_preview),
    }
)
```

### 6. Partitioning

**Common Partition Definitions:**
```python
# Daily partitions
from dagster import DailyPartitionsDefinition

daily_partition = DailyPartitionsDefinition(start_date="2024-01-01")

# Access in asset
def my_asset(context: dg.AssetExecutionContext):
    window = context.asset_partitions_time_window_for_output()
    start_date = window.start
    end_date = window.end
```

### 7. Resources

**Common Resources:**
- `SnowflakeResource` - Snowflake database access
- `InsightsBigQueryResource` - BigQuery for Dagster Insights
- `FivetranResource` - Fivetran API integration
- `SegmentResource` - Segment API access
- Environment-specific resource configuration via `utils/environment_helpers.py`

## Integration Patterns

### Fivetran
- Component-based integration in `lib/fivetran/component.py`
- Syncs external data sources to Snowflake
- Automated freshness checks and connection tests

### dbt
- Full dbt project integration in `dagster_open_platform_dbt/`
- Custom translator in `lib/dbt/translator.py`
- Asset dependencies automatically mapped from dbt DAG

### Snowflake
- Native Snowflake assets for data transformations
- Component-based pattern for common queries
- Connection pooling and credential management

### Sling
- Sling-based data ingestion from various sources
- Egress patterns for data export
- Cloud product ingestion utilities

### Segment
- Event data ingestion from Segment
- Real-time and batch processing patterns

## Development Workflows

### Creating a New Asset

1. **Choose location:** `defs/<integration>/py/assets/` or `defs/<integration>/assets/`
2. **Define asset:**
   ```python
   @dg.asset(
       key=["database", "schema", "table_name"],
       group_name="my_group",
       automation_condition=dg.AutomationCondition.eager(),
   )
   def my_new_asset(context, snowflake):
       """Clear description of what this asset does."""
       # Implementation
       return dg.MaterializeResult(metadata={...})
   ```
3. **Add checks:** Define `check_specs` for data quality
4. **Test locally:** Use `dagster dev` or `just user-cloud`
5. **Format & check:** Run `just ruff` and `just pyright`

### Creating a New Component

1. **Define component class** in `lib/<integration>/component.py`:
   ```python
   from dagster.components import Component, component

   @component(name="my_component")
   class MyComponent(Component):
       def build_defs(self, context):
           # Return Definitions object
           return dg.Definitions(assets=[...])
   ```
2. **Register in pyproject.toml:**
   ```toml
   [tool.dg.project]
   registry_modules = [
       "dagster_open_platform.lib.*",
   ]
   ```
3. **Create YAML config** in `defs/<integration>/components/component.yaml`
4. **Test and iterate**

### Environment Configuration

**Environment Helpers:**
```python
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)

database = get_database_for_environment()  # "PROD", "DEV", etc.
schema = get_schema_for_environment("analytics")
```

**Environment Variables:**
- Required env vars documented in `component.yaml` under `requirements.env`
- Use `{{ env('VAR_NAME') }}` in component YAML for templating

### Testing

**Test Structure:**
```
dagster_open_platform_tests/
├── unit/           # Unit tests for utilities and components
├── integration/    # Integration tests with external systems
└── fixtures/       # Shared test fixtures
```

**Running Tests:**
```bash
# From dagster-open-platform directory
pytest dagster_open_platform_tests/

# Run specific test
pytest dagster_open_platform_tests/unit/test_my_module.py

# With coverage
pytest --cov=dagster_open_platform dagster_open_platform_tests/
```

**Testing Best Practices:**
- Mock external API calls using `responses` or `mock`
- Use snapshot testing with `syrupy` for complex outputs
- Test component instantiation and definition generation
- Test asset execution with mock resources

### Code References

The project automatically links code to Git using `link_defs_code_references_to_git()` in `definitions.py`. This enables:
- Click-through from Dagster UI to source code
- Version tracking for deployed assets
- Enhanced debugging and collaboration

## Common Patterns

### Pattern: Multi-Environment Deployment

Assets adapt to environment (prod, dev, staging) using helpers:
```python
database = get_database_for_environment()
schema = get_schema_for_environment("my_schema")
```

### Pattern: Incremental Loading

For large datasets, use partitions with incremental logic:
```python
@dg.asset(partitions_def=daily_partition)
def incremental_asset(context):
    window = context.asset_partitions_time_window_for_output()
    # Query only for the partition time window
```

### Pattern: Asset Dependencies

Declare dependencies explicitly:
```python
@dg.asset(deps=[upstream_asset_key])
def downstream_asset(context):
    # This asset depends on upstream_asset_key
```

Or via key references:
```python
upstream_data = context.resources.snowflake.fetch_table(upstream_key)
```

### Pattern: Asset Groups

Organize related assets:
```python
@dg.asset(group_name="oss_analytics")
def pypi_downloads(): ...

@dg.asset(group_name="oss_analytics")
def github_metrics(): ...
```

## Key Files

- **`definitions.py`** - Root definitions entry point, loaded by Dagster
- **`lib/executable_component.py`** - Base component classes
- **`utils/environment_helpers.py`** - Environment configuration
- **`utils/source_code.py`** - Git integration for code references
- **`pyproject.toml`** - Package config, dependencies, tool settings

## Development Commands

From the repository root (`/Users/nicklausroach/Projects/core_repos/internal`):

```bash
# Sync environment
just sync

# Format code
just ruff

# Type check
just pyright

# Run all checks
just check

# Run local Dagster UI
just user-cloud  # User-facing cloud
just host-cloud  # Host-facing cloud

# Run tests
cd python_modules/dagster-open-platform && pytest dagster_open_platform_tests/
```

## Troubleshooting

### Missing Dependencies
- Run `just sync` to sync all workspace dependencies
- Check `pyproject.toml` for required packages

### Environment Variables
- Check `component.yaml` for required environment variables
- Ensure variables are set in your shell or `.env` file

### Type Errors
- Run `just pyright` to catch type issues
- Ensure proper type annotations for resources and context

### Asset Not Loading
- Verify `definitions.py` imports the asset's module
- Check component YAML syntax and registration
- Look for errors in `dagster dev` logs

## Additional Resources

- **Main Documentation:** See `README.md` in this directory
- **Dagster Docs:** https://docs.dagster.io
- **OSS Repository:** `../dagster` (sibling dependency)
- **Internal Tools:** `just dagster-labs` for admin utilities

## Notes for Claude Code

When working with this codebase:
1. **Follow existing patterns** - Look at similar assets/components for reference
2. **Use type hints** - All code should be properly typed for pyright
3. **Add metadata** - Include rich metadata in MaterializeResult for observability
4. **Document assets** - Write clear docstrings explaining business purpose
5. **Test thoroughly** - Add tests for new components and complex assets
6. **Check environment** - Use environment helpers for multi-environment support
7. **Format before committing** - Always run `just ruff` before committing
