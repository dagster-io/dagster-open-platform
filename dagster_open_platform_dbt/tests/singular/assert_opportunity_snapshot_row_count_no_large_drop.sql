-- Detects day-over-day row count drops greater than 15% in salesforce_opportunities_daily_snapshot.
-- A large drop indicates a partial or failed Fivetran ingestion run.
-- Checks the last 30 days to catch recent regressions.
--
-- Returns rows (test failure) for any date where the count dropped more than 15% vs the prior day.

with daily_counts as (
    select
        snapshot_date,
        count(*) as row_count
    from {{ ref('salesforce_opportunities_daily_snapshot') }}
    where snapshot_date >= dateadd(day, -31, current_date)
        and snapshot_date <= dateadd(day, -2, current_date)
    group by 1
),

with_prior_day as (
    select
        snapshot_date,
        row_count,
        lag(row_count) over (order by snapshot_date) as prior_row_count
    from daily_counts
)

select
    snapshot_date,
    row_count,
    prior_row_count,
    round((prior_row_count - row_count) / prior_row_count * 100, 1) as pct_drop
from with_prior_day
where prior_row_count is not null
    and row_count < prior_row_count * 0.85
