{{ config(snowflake_warehouse="L_WAREHOUSE") }}
with event_logs as (
    select * from {{ ref('stg_postgres__event_logs') }}
),

run_tags as (
    select * from {{ ref('stg_postgres__run_tags') }}
),

runs as (
    select * from {{ ref('stg_postgres__runs') }}
),

events as (
    select
        event_start.organization_id,
        event_start.deployment_id,
        event_start.run_id,
        event_start.created_at as started_at,
        event_end.created_at as ended_at,
        timestampdiff('ms', started_at, ended_at) as duration_ms

    from event_logs as event_start,
        lateral (
            select min(created_at) as created_at
            from event_logs
            where
                dagster_event_type in ('PIPELINE_SUCCESS', 'PIPELINE_FAILURE', 'PIPELINE_CANCELED')
                and event_logs.run_id = event_start.run_id
                and event_logs.organization_id = event_start.organization_id
                and event_logs.deployment_id = event_start.deployment_id
        ) as event_end
    where event_start.dagster_event_type = 'PIPELINE_START'
    qualify
        row_number()
            over (
                partition by organization_id, deployment_id, run_id -- noqa: RF02
                order by event_start.created_at
            )
        = 1
),

tags as (
    select
        organization_id,
        deployment_id,
        run_id,
        max(case when key = '.dagster/agent_type' then value end) as agent_type,
        max(case when key = 'dagster/backfill' then value end) as backfill_id,
        max(case when key = 'dagster/partition_set' then value end) as partition_set_name,
        max(case when key = 'dagster/schedule_name' then value end) as schedule_name,
        max(case when key = 'dagster/sensor_name' then value end) as sensor_name
    from run_tags
    where
        key in (
            '.dagster/agent_type',
            'dagster/backfill',
            'dagster/partition_set',
            'dagster/schedule_name',
            'dagster/sensor_name'
        )
    group by 1, 2, 3

),


final as (

    select

        runs.organization_id,
        runs.deployment_id,
        runs.run_id,

        runs.job_name,
        runs.status as run_status,
        runs.repository_name,
        runs.code_location_name,

        coalesce(tags.agent_type, 'HYBRID') as agent_type,

        tags.backfill_id,
        tags.partition_set_name,

        tags.schedule_name,
        tags.sensor_name,

        runs.created_at,
        runs.updated_at,

        events.started_at,
        events.ended_at,
        events.duration_ms,
        events.duration_ms / 1000 / 60.0 as duration_mins

    from runs
    left join events using (organization_id, deployment_id, run_id)
    left join tags using (organization_id, deployment_id, run_id)

)

select * from final
