-- depends_on: {{ ref('reporting_asset_materialization_metrics_rollup_stats') }}

with num_orgs_per_day_major_decrease as (
    select
        rollup_date,
        count(organization_id) as num_orgs
    from {{ ref('reporting_asset_materialization_metrics_rollup_stats') }}
    where
        num_metrics_daily_percent_change < -50 -- num metrics decreased by more than 50% compared to the previous day
        and rollup_date > current_date - 14 -- only look at the last 14 days
    group by rollup_date
)


select * from num_orgs_per_day_major_decrease where num_orgs > 5