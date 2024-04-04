{{
  config(
    snowflake_warehouse="L_WAREHOUSE",
    materialized='incremental',
    unique_key='step_data_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}

with steps as (
    select * from {{ ref('fct_steps') }}
    where not is_anomaly
),

event_logs as (
    select * from {{ ref('stg_postgres__event_logs') }}
),

execution_time_metadata as (
    select * from {{ ref('execution_time_metadata') }}
),

step_retries as (
    select
        run_id,
        step_key,
        count(*) as step_retries
    from event_logs
    where dagster_event_type = 'STEP_RESTARTED'
    group by all
),

step_failures as (
    select
        run_id,
        step_key,
        count(*) as step_failures
    from event_logs
    where dagster_event_type = 'STEP_FAILURE'
    group by all
),

observations as (
    select
        run_id,
        step_key,
        count(*) as asset_observations
    from event_logs
    where dagster_event_type = 'ASSET_OBSERVATION'
    group by all
),

runless_observations as (
    select
        run_id,
        created_at as run_ended_at,
        created_at as run_started_at,
        organization_id,
        deployment_id,
        count(*) as runless_asset_observations
    from event_logs
    where dagster_event_type = 'ASSET_OBSERVATION' and run_id = ''
    group by all
),

materializations as (
    select
        run_id,
        step_key,
        count(*) as asset_materializations
    from event_logs
    where dagster_event_type = 'ASSET_MATERIALIZATION'
    group by all
),

runless_materializations as (
    select
        run_id,
        created_at as run_ended_at,
        created_at as run_started_at,
        organization_id,
        deployment_id,
        count(*) as runless_asset_materializations
    from event_logs
    where dagster_event_type = 'ASSET_MATERIALIZATION' and run_id = ''
    group by all
),

asset_check_errors as (
    select
        run_id,
        step_key,
        count(*) as asset_check_errors
    from event_logs
    where
        dagster_event_type = 'ASSET_CHECK_EVALUATION'
        and parse_json(event_data):dagster_event:event_specific_data:success = 'false'
        and parse_json(event_data):dagster_event:event_specific_data:severity:__enum__::string
        = 'AssetCheckSeverity.ERROR'
    group by all
),

asset_check_warnings as (
    select
        run_id,
        step_key,
        count(*) as asset_check_warnings
    from event_logs
    where
        dagster_event_type = 'ASSET_CHECK_EVALUATION'
        and parse_json(event_data):dagster_event:event_specific_data:success = 'false'
        and parse_json(event_data):dagster_event:event_specific_data:severity:__enum__::string
        = 'AssetCheckSeverity.WARN'
    group by all
),

-- We compute the total execution time that is specifically
-- allocated to specific assets. This is often less than the
-- total execution time, since often compute is not attributed to a
-- specific asset.
asset_execution_time as (
    select
        run_id,
        step_key,
        sum(execution_time_s) * 1000 as total_execution_time_ms,
        count(*) as assets_with_metadata
    from execution_time_metadata
    group by run_id, step_key
),

check_steps as (
    select
        run_id,
        step_key,
        count(*) as asset_checks
    from event_logs
    where dagster_event_type in ('ASSET_CHECK_EVALUATION', 'ASSET_CHECK_EVALUATION_PLANNED')
    group by all
),

metrics_joined as (
    select
        coalesce(
            steps.organization_id,
            runless_observations.organization_id,
            runless_materializations.organization_id
        ) as organization_id,
        coalesce(
            steps.deployment_id,
            runless_observations.deployment_id,
            runless_materializations.deployment_id
        ) as deployment_id,
        coalesce(
            steps.run_id,
            runless_observations.run_id,
            runless_materializations.run_id
        ) as run_id,
        steps.step_id,
        steps.step_data_id,
        steps._incremented_at,
        steps.step_key,
        coalesce(
            steps.run_ended_at,
            runless_observations.run_ended_at,
            runless_materializations.run_ended_at
        ) as run_ended_at,
        coalesce(
            steps.run_started_at,
            runless_observations.run_started_at,
            runless_materializations.run_started_at
        ) as run_started_at,

        zeroifnull(observations.asset_observations)::number as asset_observations,
        zeroifnull(runless_observations.runless_asset_observations)::number
            as runless_asset_observations,
        zeroifnull(materializations.asset_materializations)::number as asset_materializations,
        zeroifnull(runless_materializations.runless_asset_materializations)::number
            as runless_asset_materializations,
        iff(steps.step_id is null, 0, 1)::number as steps,

        zeroifnull(check_steps.asset_checks)::number as asset_checks,
        zeroifnull(steps.step_duration_mins)::number as step_duration_mins,
        zeroifnull(steps.step_duration_ms)::number as execution_time_ms,

        zeroifnull(asset_execution_time.total_execution_time_ms)::number as total_execution_time_ms,
        zeroifnull(asset_execution_time.assets_with_metadata)::number
            as assets_with_execution_time_metadata,

        zeroifnull(steps.retry_duration_ms)::number as retry_duration_ms,
        zeroifnull(step_retries.step_retries)::number as step_retries,
        zeroifnull(step_failures.step_failures)::number as step_failures,

        zeroifnull(asset_check_errors.asset_check_errors)::number as asset_check_errors,
        zeroifnull(asset_check_warnings.asset_check_warnings)::number as asset_check_warnings

    from steps
    left join step_retries using (run_id, step_key)
    left join step_failures using (run_id, step_key)
    left join observations using (run_id, step_key)
    full outer join runless_observations using (run_id, organization_id, deployment_id)
    left join materializations using (run_id, step_key)
    full outer join runless_materializations using (run_id, organization_id, deployment_id)
    left join asset_execution_time using (run_id, step_key)
    left join check_steps using (run_id, step_key)
    left join asset_check_errors using (run_id, step_key)
    left join asset_check_warnings using (run_id, step_key)
)

select

    organization_id,
    deployment_id,
    run_id,
    step_id,
    step_data_id,
    _incremented_at,
    step_key,
    run_ended_at,
    run_started_at,

    asset_observations + runless_asset_observations as observations,
    asset_materializations + runless_asset_materializations as materializations,
    steps,
    materializations + steps as dagster_credits,
    asset_checks,
    step_duration_mins,
    execution_time_ms,

    execution_time_ms - total_execution_time_ms as unaccounted_duration_ms,
    unaccounted_duration_ms / 1000 as unaccounted_duration_s,

    assets_with_execution_time_metadata,

    retry_duration_ms,
    step_retries,
    step_failures,

    asset_check_errors,
    asset_check_warnings,

    case
        when materializations = 0 and asset_checks > 0 then 1
        else 0
    end as asset_check_only_steps

from metrics_joined

{% if is_incremental() %}
    where
        run_ended_at >= '{{ var('min_date') }}'
        and run_ended_at < '{{ var('max_date') }}'
{% endif %}
