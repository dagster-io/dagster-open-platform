with local_instances as (
    select instance_id from {{ ref('telemetry_local_active_flag') }}
    group by 1
    having sum(is_active) >= 1
)


select *


from
    {{ ref('telemetry_local_daily_instances') }}
where
    instance_id in (
        select instance_id from local_instances
    )
order by 2, 1
