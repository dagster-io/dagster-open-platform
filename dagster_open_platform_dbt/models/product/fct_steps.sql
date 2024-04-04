/* This is a partitioned, incremental model. Paritioning is handled
by Dagster through the min_date and max_date variables.
Most often, this will append new rows to the existing table, but
in cases of a backfill, we'll use merge to overwrite using the
unique step_id key.

We use the `is_incremntal` tag to identify this as an incremental model,
this tag is used by Dagster's dbt asset selector to find all incremental
models *
*/

{{
  config(
    snowflake_warehouse="L_WAREHOUSE",
    materialized='incremental',
    unique_key='step_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}

with event_logs as (
    select * from {{ ref('stg_postgres__event_logs') }}
),

steps_up_for_retry as (
    select
        run_id,
        step_key,
        created_at
    from event_logs
    where dagster_event_type = 'STEP_UP_FOR_RETRY'
),

steps_restarted as (
    select
        run_id,
        step_key,
        created_at
    from event_logs
    where dagster_event_type = 'STEP_RESTARTED'
),

retries as (
    select
        steps_up_for_retry.run_id,
        steps_up_for_retry.step_key,
        steps_up_for_retry.created_at as start_time,
        min(steps_restarted.created_at) as end_time
    from steps_up_for_retry
    left join steps_restarted
        on
            steps_up_for_retry.run_id = steps_restarted.run_id
            and steps_up_for_retry.step_key = steps_restarted.step_key
            and steps_up_for_retry.created_at < steps_restarted.created_at
    group by 1, 2, 3
),

retry_durations as (
    select
        run_id,
        step_key,
        sum(timestampdiff('ms', start_time, end_time)) as retry_duration_ms
    from retries
    group by 1, 2
),

step_execution_starts as (
    select
        run_id,
        step_key,
        min(created_at) as start_time
    from event_logs
    where dagster_event_type = 'STEP_START'
    group by 1, 2
),

step_execution_ends as (
    select
        run_id,
        step_key,
        dagster_event_type,
        created_at as end_time,
        rank() over (partition by run_id, step_key order by created_at asc) as ranking
    from event_logs
    where dagster_event_type in ('STEP_SUCCESS', 'STEP_FAILURE')
),

step_worker_init_starts as (
    select
        run_id,
        step_key,
        min(created_at) as start_time
    from event_logs
    where dagster_event_type = 'STEP_WORKER_STARTING'
    group by 1, 2
),

step_worker_init_ends as (
    select
        run_id,
        step_key,
        min(created_at) as end_time
    from event_logs
    where dagster_event_type = 'STEP_WORKER_STARTED'
    group by 1, 2
),

resource_init_starts as (
    select
        run_id,
        step_key,
        min(created_at) as start_time
    from event_logs
    where dagster_event_type = 'RESOURCE_INIT_STARTED'
    group by 1, 2
),

resource_init_ends as (
    select
        run_id,
        step_key,
        dagster_event_type,
        created_at as end_time,
        rank() over (partition by run_id, step_key order by created_at asc) as ranking
    from event_logs
    where dagster_event_type in ('RESOURCE_INIT_SUCCESS', 'RESOURCE_INIT_FAILURE')
),

step_execution_durations as (
    select
        ss.run_id,
        ss.step_key,
        ss.start_time,
        se.end_time,
        se.dagster_event_type as status,
        timestampdiff('ms', ss.start_time, se.end_time) as duration_ms
    from step_execution_starts as ss
    left join step_execution_ends as se
        on
            ss.run_id = se.run_id
            and ss.step_key = se.step_key
            and se.ranking = 1
),

step_init_durations as (
    select
        ws.run_id,
        ws.step_key,
        re.dagster_event_type as status,
        timestampdiff('ms', ws.start_time, we.end_time) as step_worker_init_duration_ms,
        timestampdiff('ms', rs.start_time, re.end_time) as resource_init_duration_ms
    from step_worker_init_starts as ws
    left join step_worker_init_ends as we
        on
            ws.run_id = we.run_id
            and ws.step_key = we.step_key
    left join resource_init_starts as rs
        on
            ws.run_id = rs.run_id
            and ws.step_key = rs.step_key
    left join resource_init_ends as re
        on
            ws.run_id = re.run_id
            and ws.step_key = re.step_key
            and re.ranking = 1
),

step_durations as (
    select
        coalesce(se.run_id, si.run_id) as run_id,
        coalesce(se.step_key, si.step_key) as step_key,
        coalesce(se.status, si.status) as status,
        se.start_time,
        se.end_time,
        coalesce(se.duration_ms, 0) as duration_ms,
        coalesce(si.step_worker_init_duration_ms, 0) as step_worker_init_duration_ms,
        coalesce(si.resource_init_duration_ms, 0) as resource_init_duration_ms,
        coalesce(rd.retry_duration_ms, 0) as retry_duration_ms
    from step_execution_durations as se
    full outer join step_init_durations as si
        on se.run_id = si.run_id and se.step_key = si.step_key
    left join retry_durations as rd
        on se.run_id = rd.run_id and se.step_key = rd.step_key
),

total_step_durations as (
    select
        sd.run_id,
        (sum(sd.duration_ms) + sum(sd.resource_init_duration_ms) - sum(sd.retry_duration_ms))
        / 1000
        / 60 as total_step_duration_mins
    from step_durations as sd
    group by 1
)

select distinct
    r.organization_id,
    r.deployment_id,
    {{ dbt_utils.generate_surrogate_key(['sd.run_id', 'sd.step_key']) }} as step_id,
    uuid_string('82aa6994-b73d-4874-bfbe-fcfadebe6968', step_id) as step_data_id,
    sd.run_id,
    sd.step_key,
    r.agent_type,
    r.repository_name,
    r.code_location_name,
    r.job_name,
    sd.start_time,
    sd.end_time,
    sd.status,
    sd.duration_ms,
    sd.step_worker_init_duration_ms,
    sd.resource_init_duration_ms,
    sd.retry_duration_ms,
    r.started_at as run_started_at,
    r.ended_at as run_ended_at,
    (sd.duration_ms + sd.resource_init_duration_ms - sd.retry_duration_ms) as step_duration_ms,
    (sd.duration_ms + sd.resource_init_duration_ms - sd.retry_duration_ms)
    / 1000
    / 60 as step_duration_mins,
    tsd.total_step_duration_mins >= 100000 as is_anomaly,
    current_timestamp() as _incremented_at

from step_durations as sd
inner join {{ ref("fct_runs") }} as r
    on sd.run_id = r.run_id
left join total_step_durations as tsd
    on sd.run_id = tsd.run_id

{% if is_incremental() %}
    where r.ended_at >= '{{ var('min_date') }}' and r.ended_at < '{{ var('max_date') }}'
{% endif %}
