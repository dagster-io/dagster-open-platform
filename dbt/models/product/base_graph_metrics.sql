{{ config(snowflake_warehouse="L_WAREHOUSE") }}

with base_step_metrics as (
    select
        split_part(step_key, '.', 1) as graph_name,
        *
    from {{ ref('base_step_metrics') }}
    where step_key like '%.%'
)

select
    base_step_metrics.graph_name,
    base_step_metrics.organization_id,
    base_step_metrics.deployment_id,
    base_step_metrics.run_id,
    max(base_step_metrics._incremented_at) as _incremented_at,

    sum(base_step_metrics.dagster_credits) as dagster_credits,
    sum(base_step_metrics.step_duration_mins) as step_duration_mins,
    sum(base_step_metrics.execution_time_ms) as execution_time_ms,
    sum(base_step_metrics.unaccounted_duration_ms) as unaccounted_duration_ms,
    sum(base_step_metrics.unaccounted_duration_s) as unaccounted_duration_s,
    sum(base_step_metrics.assets_with_execution_time_metadata)
        as assets_with_execution_time_metadata,
    sum(base_step_metrics.retry_duration_ms) as retry_duration_ms,
    sum(base_step_metrics.step_retries) as step_retries,
    sum(base_step_metrics.step_failures) as step_failures,
    sum(base_step_metrics.observations) as observations,
    sum(base_step_metrics.materializations) as materializations,
    sum(base_step_metrics.asset_check_errors) as asset_check_errors,
    sum(base_step_metrics.asset_check_warnings) as asset_check_warnings


from base_step_metrics group by 1, 2, 3, 4
