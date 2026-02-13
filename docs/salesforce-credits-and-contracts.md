# Salesforce Credits and Contracts Data Model

This document describes how credits and minutes are tracked across Salesforce contracts, opportunities, and quotes in the `salesforce_contracts` dbt model.

## Overview

The `salesforce_contracts` table consolidates data from multiple Salesforce objects to provide a complete view of contracted and upsold credits/minutes across customer contracts.

## Core Entities

### 1. Contracts
- **Source**: `stg_salesforce__contracts`
- **Primary entity**: Represents a legal agreement with specific start/end dates
- **Key fields**: `contract_id`, `opportunity_id`, `account_id`, `contract_start_date`, `contract_end_date`
- **Relationship**: Multiple contracts can belong to the same opportunity (multi-year deals with separate terms)

### 2. Opportunities
- **Source**: `salesforce_opportunities`
- **Types**:
  - **New Business**: Initial customer acquisition
  - **Renewal**: Contract renewal
  - **Upsell**: Additional credits/capacity sold to existing customers
- **Key fields**: `opportunity_id`, `account_id`, `opportunity_type`, `close_date`, `is_closed`, `is_won`

### 3. Quotes
- **Source**: `salesforce_quote`
- **Purpose**: Formal price quotation linked to an opportunity
- **Key fields**: `quote_id`, `opportunity_id`, `is_syncing`
- **Note**: Only quotes with `is_syncing = true` are included (these represent the active/winning quote)

### 4. Quote Line Items
- **Source**: `salesforce_quote_line_item`
- **Purpose**: Individual products within a quote
- **Key fields**: `quote_id`, `product_name`, `quantity`, `term`
- **Product types**:
  - Credits products (matched by `product_name LIKE '%Credits%'`)
  - Minutes products (matched by `product_name LIKE '%Minutes%'`)

## Credits and Minutes Types

### 1. Quoted Contracted Credits/Minutes

**What they are**: Credits and minutes from the original deal (New Business or Renewal opportunities)

**Source**: Quote line items from New Business and Renewal opportunities where:
- `opportunity_type IN ('New Business', 'Renewal')`
- `is_closed = true` and `is_won = true`
- `close_date >= 2024` (historical cutoff)
- Quote is syncing

**Attribution logic**:
- Grouped by opportunity and term order
- Matched to contracts by `opportunity_id` and aligned by term order
- Each contract term gets the quoted credits/minutes for its corresponding term in the quote

**Fields**:
- `quoted_contracted_credits`: Credits for this contract term
- `quoted_contracted_minutes`: Minutes for this contract term
- `sum_previous_terms_quoted_contracted_credits`: Cumulative sum of quoted credits from all prior terms in the same opportunity
- `sum_future_terms_quoted_contracted_credits`: Cumulative sum of quoted credits from all future terms in the same opportunity
- Similar fields for minutes

### 2. Upsell Credits/Minutes

**What they are**: Additional credits and minutes purchased through Upsell opportunities during an existing contract period

**Source**: Quote line items from Upsell opportunities where:
- `opportunity_type = 'Upsell'`
- `is_closed = true` and `is_won = true`
- Quote is syncing

**Attribution logic** (most complex part):

1. **Find upsell opportunities with their credits**:
   ```sql
   -- Extract credits/minutes from upsell quote line items
   SELECT
     opportunity_id,
     account_id,
     close_date,
     SUM(credits) as upsell_credits,
     SUM(minutes) as upsell_minutes
   FROM upsell_opportunities
   JOIN quotes JOIN quote_line_items
   ```

2. **Match upsells to contract opportunities**:
   ```sql
   -- Find which opportunity the upsell belongs to
   -- by checking if close_date falls within ANY contract period
   JOIN contracts
     ON upsell.account_id = contract.account_id
     AND upsell.close_date BETWEEN contract.start_date AND contract.end_date
   ```

3. **Attribute to the LAST contract in the matched opportunity**:
   ```sql
   -- Within the matched opportunity, find the LAST contract
   ROW_NUMBER() OVER (
     PARTITION BY upsell_opportunity_id
     ORDER BY contract_end_date DESC
   ) = 1
   ```

**Why the last contract?**
- Upsells sold during an earlier term should be attributed to the final term
- This reflects the full value realized over the customer relationship
- Provides better visibility into total capacity by the end of the contract period

**Fields**:
- `upsell_credits`: Credits from upsell opportunities attributed to this contract
- `upsell_minutes`: Minutes from upsell opportunities attributed to this contract
- `sum_prior_terms_upsell_credits`: Cumulative sum of upsell credits from all prior contract terms in the same opportunity
- `sum_future_terms_upsell_credits`: Cumulative sum of upsell credits from all future contract terms in the same opportunity
- Similar fields for minutes

## Example Scenario

### Multi-Year Deal with Upsell

**Setup**:
- **Opportunity A** (New Business): 3-year deal
  - Contract 1: Jan 2024 - Dec 2024 (Term 1)
  - Contract 2: Jan 2025 - Dec 2025 (Term 2)
  - Contract 3: Jan 2026 - Dec 2026 (Term 3)
  - Quoted credits: 1M per year

- **Opportunity B** (Upsell): Closes Aug 2024
  - Additional 500K credits sold

**Resulting attribution**:

| Contract | Term | Quoted Credits | Upsell Credits | Sum Prior Terms Upsell | Sum Future Terms Upsell |
|----------|------|----------------|----------------|------------------------|-------------------------|
| Contract 1 | 1 | 1M | 0 | 0 | 500K |
| Contract 2 | 2 | 1M | 0 | 0 | 500K |
| Contract 3 | 3 | 1M | **500K** | 0 | 0 |

**Explanation**:
- The upsell closed in Aug 2024 (during Term 1's period)
- But it's attributed to Contract 3 (the LAST contract in Opportunity A)
- Contract 1 shows `sum_future_terms_upsell_credits = 500K` indicating upsells coming in later terms
- Contract 3 gets the actual `upsell_credits = 500K`

## Key SQL CTEs

### `quoted_by_term`
Extracts quoted credits/minutes from New Business and Renewal quote line items, grouped by opportunity and term.

### `upsell_credits_by_opp`
Extracts upsell credits/minutes from Upsell quote line items, grouped by upsell opportunity.

### `upsell_to_opportunity_match`
Matches each upsell to a contract opportunity by checking if the upsell close date falls within any contract period for that account.

### `upsell_to_last_contract`
For each matched upsell-to-opportunity pairing, finds ALL contracts in that opportunity and ranks them by end date (descending) to identify the last contract.

### `upsell_to_last_contract_filtered`
Filters to keep only the last contract (rank = 1) for each upsell.

### `upsell_by_contract`
Aggregates all upsells attributed to each contract (in case multiple upsells map to the same final contract).

### `contract_sums`
Combines quoted credits and upsell credits, calculating cumulative sums across contract terms using window functions.

## Window Function Logic

**Cumulative sums are calculated using**:
```sql
SUM(value) OVER (
  PARTITION BY opportunity_id
  ORDER BY contract_start_date
  ROWS BETWEEN unbounded preceding AND 1 preceding  -- for prior terms
  -- OR --
  ROWS BETWEEN 1 following AND unbounded following   -- for future terms
)
```

This provides:
- **Prior terms**: Sum of all previous contracts in the opportunity
- **Future terms**: Sum of all upcoming contracts in the opportunity
- Useful for understanding total capacity over time and forecasting

## Data Quality Notes

1. **Quote syncing**: Only syncing quotes are included to avoid double-counting multiple quotes for the same opportunity

2. **Close date filter**: Quoted contracted credits only include opportunities closed in 2024+ to avoid historical data quality issues

3. **Null handling**: All sums use `COALESCE(..., 0)` to ensure consistent numeric values

4. **Term alignment**: Contracts and quotes are matched by term order (1st contract → 1st term, etc.)

## Related Files

- **dbt model**: `python_modules/dagster-open-platform/dagster_open_platform_dbt/models/purina/model/salesforce/salesforce_contracts.sql`
- **YAML documentation**: `python_modules/dagster-open-platform/dagster_open_platform_dbt/models/purina/model/salesforce/_salesforce.yml`
- **Staging tables**:
  - `stg_salesforce__contracts`
  - `salesforce_opportunities`
  - `salesforce_quote`
  - `salesforce_quote_line_item`

## Common Queries

### Find total capacity for an account
```sql
SELECT
  account_name,
  contract_id,
  contract_start_date,
  contract_end_date,
  quoted_contracted_credits + upsell_credits AS total_credits,
  quoted_contracted_minutes + upsell_minutes AS total_minutes
FROM purina.model.salesforce_contracts
WHERE account_id = '<account_id>'
ORDER BY contract_start_date
```

### See upsell attribution for an opportunity
```sql
SELECT
  contract_id,
  contract_start_date,
  contract_end_date,
  upsell_credits,
  sum_prior_terms_upsell_credits,
  sum_future_terms_upsell_credits
FROM purina.model.salesforce_contracts
WHERE opportunity_id = '<opportunity_id>'
ORDER BY contract_start_date
```

## Technical Notes

### Snowflake Data Analyst Agent
When working with this data in future sessions, use the `snowflake-data-analyst` agent which has been configured to query Snowflake via the `snow` CLI tool:

```bash
snow sql -c default -q "YOUR_QUERY_HERE"
```

The agent cannot use MCP tools when spawned as a sidechain, so it relies on the Snowflake CLI instead.

### Testing in Sandbox
The sandbox schema `sandbox.nickroach.salesforce_contracts` contains a development copy for testing changes before deploying to production.

## Future Considerations

1. **Minutes tracking**: Currently tracked similarly to credits but may have different business logic requirements

2. **Multi-currency**: Current model assumes single currency; may need enhancement for international deals

3. **Partial term upsells**: Current logic attributes entire upsell to last contract; could be refined to prorate based on contract periods

4. **Historical data**: Pre-2024 data excluded from quoted contracted credits but included in upsells - may want consistency

---

*Last updated: 2026-02-13*
*Author: Claude Sonnet 4.5*
