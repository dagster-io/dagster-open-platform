-- Detects gaps in the salesforce_opportunities_daily_snapshot date sequence.
-- Checks the last 30 days so newly introduced gaps are caught without surfacing
-- historical gaps that have already been reviewed and backfilled.
--
-- Returns rows (test failure) for any date that is missing from the snapshot table.

with date_spine as (
    select
        dateadd(day, seq4(), dateadd(day, -31, current_date)) as expected_date
    from table(generator(rowcount => 30))
),

existing_dates as (
    select distinct snapshot_date
    from {{ ref('salesforce_opportunities_daily_snapshot') }}
    where snapshot_date >= dateadd(day, -31, current_date)
        and snapshot_date <= dateadd(day, -2, current_date)
)

select
    d.expected_date as missing_snapshot_date
from date_spine d
left join existing_dates e on d.expected_date = e.snapshot_date
where e.snapshot_date is null
