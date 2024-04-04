{{ config(snowflake_warehouse="L_WAREHOUSE") }}
with runs as (
    select * from {{ ref('fct_runs') }}
),

base as (
    select * from {{ ref('base_step_metrics') }}
),

base_agg as (
    select

        organization_id,
        deployment_id,
        run_id,
        max(run_ended_at) as ended_at,
        sum(materializations) as materializations,
        sum(step_duration_mins) as step_duration_mins,
        sum(steps) as steps,
        sum(asset_check_only_steps) as asset_check_only_steps,
        sum(asset_checks) as asset_checks

    from base
    group by all
),

run_metrics as (
    select

        organization_id,
        run_id,
        agent_type,
        ended_at,
        sum(duration_mins) as run_duration_mins,
        count(*) as runs

    from runs
    group by all
),

combined as (

    select

        date_trunc(
            'day',
            coalesce(run_metrics.ended_at, base_agg.ended_at)
        ) as ds,
        base_agg.organization_id,
        base_agg.run_id,
        {{ dbt_utils.generate_surrogate_key(["ds", "organization_id", "run_id"]) }}
            as surrogate_key,
        run_metrics.agent_type,
        coalesce(sum(base_agg.materializations), 0) as materializations,
        coalesce(sum(base_agg.step_duration_mins), 0) as step_duration_mins,
        coalesce(sum(base_agg.steps), 0) as steps,
        coalesce(sum(base_agg.asset_checks), 0) as asset_checks,
        coalesce(sum(base_agg.asset_check_only_steps), 0) as asset_check_only_steps,
        coalesce(sum(run_metrics.run_duration_mins), 0) as run_duration_mins,
        coalesce(sum(run_metrics.runs), 0) as runs,

        /* Metric Definitions for usage/reporting */
        steps - asset_check_only_steps as steps_credits

    from base_agg
    left join run_metrics using (organization_id, run_id)
    where ds is not null
    group by all

)

select * from combined
