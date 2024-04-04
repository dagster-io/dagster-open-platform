{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=["insights"]
  )
}}
with asset_materialization_metrics as (
    select * from {{ ref('reporting_asset_materialization_metrics') }}
),

step_data as (
    select * from {{ ref('reporting_step_data') }}
),

asset_metrics_rollup as (
    select
        rsd.organization_id,
        rsd.deployment_id,
        rsd.repository_name,
        rsd.code_location_name,
        amm.asset_key,
        -- make sure we don't ever have 2 different asset groups for the same asset key
        max_by(amm.asset_group, amm.last_rebuilt) as asset_group,
        amm.metric_name,
        date_trunc('day', rsd.step_end_timestamp) as rollup_date,
        avg(amm.metric_value / amm.metric_multi_asset_divisor) as metric_value_avg,
        sum(amm.metric_value / amm.metric_multi_asset_divisor) as metric_value_sum,
        percentile_cont(0.75) within
        group (order by amm.metric_value / amm.metric_multi_asset_divisor)
            as metric_value_p75,
        percentile_cont(0.90) within
        group (order by amm.metric_value / amm.metric_multi_asset_divisor)
            as metric_value_p90,
        percentile_cont(0.95) within
        group (order by amm.metric_value / amm.metric_multi_asset_divisor)
            as metric_value_p95,
        percentile_cont(0.99) within
        group (order by amm.metric_value / amm.metric_multi_asset_divisor)
            as metric_value_p99,
        array_agg(amm.metric_value / amm.metric_multi_asset_divisor)
            as metric_values,
        max(amm.last_rebuilt) as last_rebuilt,
        null as run_ids

    from
        asset_materialization_metrics as amm
    left join step_data as rsd
        on amm.step_data_id = rsd.id

    where date_trunc('day', rsd.step_end_timestamp) > '2023-06-01'
    {% if is_incremental() %}
        and amm.run_ended_at >= '{{ var('min_date') }}' and amm.run_ended_at < '{{ var('max_date') }}'
    {% endif %}

    group by
        all
)

select
    *,
    {{ dbt_utils.generate_surrogate_key([
        'organization_id',
        'deployment_id',
        'repository_name',
        'code_location_name',
        'asset_key',
        'metric_name',
        'rollup_date'
    ]) }} as unique_key
from asset_metrics_rollup
