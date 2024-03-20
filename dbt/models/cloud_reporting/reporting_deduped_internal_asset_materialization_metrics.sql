{{
  config(
    snowflake_warehouse="L_WAREHOUSE",
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
        run_ended_at

    from pivot
    where step_data_id in (
        select id from step_data
    )
)

-- Deduplicate materialization events for the same asset + partition in one step

select

    {{ dbt_utils.generate_surrogate_key([
        'ame.organization_id',
        'ame.deployment_id',
        'ame.step_data_id',
        'ame.asset_key',
        'ame.metric_name',
        'ame.partition'
    ]) }} as unique_key,
    ame.organization_id,
    ame.deployment_id,
    ame.step_data_id,
    ame.asset_key,
    ame.asset_group,
    ame.metric_name,
    ame.partition,
    sum(ame.metric_value) as metric_value,
    max(ame.last_rebuilt) as last_rebuilt,
    max(ame.metric_multi_asset_divisor) as metric_multi_asset_divisor,
    ame.run_ended_at,
    step_data.run_id

from raw_internal_asset_materialization_events as ame
left join step_data on ame.step_data_id = step_data.id

{% if is_incremental() %}
    where ame.run_ended_at >= '{{ var('min_date') }}' and ame.run_ended_at < '{{ var('max_date') }}'
{% endif %}

group by all
