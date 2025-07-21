-- Test to identify days where 99% or more organizations are not considered weekly active
-- This test will return rows (indicating failure) when the condition is met

with daily_aggregates as (
    select
        date_day,
        sum(case when is_weekly_active_organization then 1 else 0 end) as wao_orgs,
        count(organization_id) as total_orgs,
    from {{ ref('organizations_by_day') }}
    where date_day >= '2024-01-01'
    group by all
),

daily_percentages as (
    select
        date_day,
        total_orgs,
        wao_orgs,
        round((wao_orgs * 100.0) / total_orgs, 2) as percent_wao
    from daily_aggregates
)

select
    date_day,
    total_orgs,
    wao_orgs,
    percent_wao
from daily_percentages
where percent_wao <= 1.0
