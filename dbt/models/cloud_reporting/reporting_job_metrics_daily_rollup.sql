{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=["insights"]
  )
}}
with run_metrics as (
    select * from {{ ref('reporting_run_metrics') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'rrm.organization_id',
        'rrm.deployment_id',
        'rrm.repository_name',
        'rrm.code_location_name',
        'rrm.pipeline_name',
        'rrm.metric_name',
        'rrm.rollup_date'
    ]) }} as unique_key,
    rrm.organization_id,
    rrm.deployment_id,
    rrm.rollup_date,
    rrm.pipeline_name,
    rrm.metric_name,
    -- org 19 has NULLs in some repo names
    coalesce(rrm.repository_name, 'default') as repository_name,
    coalesce(rrm.code_location_name, 'default') as code_location_name,
    avg(rrm.metric_value_sum) as metric_value_avg,
    sum(rrm.metric_value_sum) as metric_value_sum,
    percentile_cont(0.75) within group (order by rrm.metric_value_sum)
        as metric_value_p75,
    percentile_cont(0.90) within group (order by rrm.metric_value_sum)
        as metric_value_p90,
    percentile_cont(0.95) within group (order by rrm.metric_value_sum)
        as metric_value_p95,
    percentile_cont(0.99) within group (order by rrm.metric_value_sum)
        as metric_value_p99,
    array_agg(rrm.metric_value_sum) as metric_values,
    max(rrm.last_rebuilt) as last_rebuilt,
    array_agg(rrm.run_id) as run_ids


from
    run_metrics as rrm

where rrm.rollup_date > '2023-06-01'

{% if is_incremental() %}
    and rrm.run_ended_at >= '{{ var('min_date') }}' and rrm.run_ended_at < '{{ var('max_date') }}'
{% endif %}

group by
    all
