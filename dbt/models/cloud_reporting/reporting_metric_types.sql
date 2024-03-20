with run_metrics as (select * from {{ ref('reporting_run_metrics') }}),

asset_materialization_metrics as (
    select * from {{ ref('reporting_asset_materialization_metrics') }}
)

(
    select
        organization_id,
        deployment_id,
        'step_metrics' as source,
        metric_name
    from
        run_metrics
    where
        run_ended_at > add_months(current_date(), -2)
    group by
        all
)
union
(
    select
        organization_id,
        deployment_id,
        'asset_materialization_metrics' as source,
        metric_name
    from
        asset_materialization_metrics
    where
        run_ended_at > add_months(current_date(), -2)
    group by
        all
)
