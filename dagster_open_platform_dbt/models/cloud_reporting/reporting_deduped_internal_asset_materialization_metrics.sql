{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}

with base_metrics as (
    select * from {{ ref('int_base_asset_metrics') }}
),

step_data as (
    select * from {{ ref('reporting_step_data') }}
),

pivot as (

    select *
    from base_metrics
    unpivot (
        metric_value for metric_name in (
            dagster_credits, execution_time_ms,
            step_retries,
            step_failures,
            materializations,
            asset_check_errors,
            asset_check_warnings
        )
    )
),

raw_internal_asset_materialization_events as (
    select

        organization_id,
        deployment_id,
        step_data_id,
        asset_key,
        asset_group,
        partition,
        case
            when pivot.metric_name = 'DAGSTER_CREDITS' then dagster_credits_divisor
            when pivot.metric_name = 'EXECUTION_TIME_S' then execution_time_divisor
            else 1
        end as metric_multi_asset_divisor,
        concat('__dagster_', lower(metric_name)) as metric_name,
        metric_value,
        _incremented_at as last_rebuilt,
        run_ended_at,
        run_id

    from pivot
    where step_data_id in (
        select id from step_data
    )
)

-- Deduplicate materialization events for the same asset + partition in one step

select

    {{ dbt_utils.generate_surrogate_key([
        'organization_id',
        'deployment_id',
        'step_data_id',
        'asset_key',
        'metric_name',
        'partition'
    ]) }} as unique_key,
    organization_id,
    deployment_id,
    step_data_id,
    asset_key,
    asset_group,
    metric_name,
    partition,
    sum(metric_value) as metric_value,
    max(last_rebuilt) as last_rebuilt,
    max(metric_multi_asset_divisor) as metric_multi_asset_divisor,
    run_ended_at,
    run_id

from raw_internal_asset_materialization_events
where {{ limit_dates_for_insights(ref_date = 'run_ended_at') }}
{% if is_incremental() %}
    and run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
{% endif %}

group by all
