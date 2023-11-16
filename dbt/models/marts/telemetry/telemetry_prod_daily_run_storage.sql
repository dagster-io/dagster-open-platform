with prod_daily_instances as (


    select
        reporting_date,
        instance_id,
        max(os_platform) as os_platform,
        max(dag_version_parsed) as dagster_version,
        sum(sensor_run_created) as num_runs_created_by_sensor,
        sum(schedule_run_created) as num_runs_created_by_schedule,
        sum(backfill_run_created) as num_runs_created_by_backfill,
        sum(run_launched_from_dagit) as num_runs_launched_from_dagit,
        sum(step_started) as num_steps_started,
        case when sum(daemon_alive) > 0 then 1 else 0 end as is_daemon_live,
        case when sum(dagit_started) > 0 then 1 else 0 end as is_dagit_started,
        case when sum(gql_query_completed) > 0 then 1 else 0 end as is_gql_query_completed,
        case when sum(is_linux) > 0 then 1 else 0 end as is_linux,
        case when sum(has_run_storage_id) > 0 then 1 else 0 end as has_run_storage_id
    from {{ ref('telemetry_prod_raw') }}
    where not is_known_ci_env
    group by 1, 2
)

select

    prod_daily_instances.reporting_date,
    telemetry_run_instance_mapping.run_storage_id,
    max(prod_daily_instances.os_platform) as os_platform,
    max(prod_daily_instances.dagster_version) as dagster_version,
    sum(prod_daily_instances.num_runs_created_by_sensor) as num_runs_created_by_sensor,
    sum(prod_daily_instances.num_runs_created_by_schedule) as num_runs_created_by_schedule,
    sum(prod_daily_instances.num_runs_created_by_backfill) as num_runs_created_by_backfill,
    sum(prod_daily_instances.num_runs_launched_from_dagit) as num_runs_launched_from_dagit,
    sum(prod_daily_instances.num_steps_started) as num_steps_started,
    max(prod_daily_instances.is_daemon_live) as is_daemon_live,
    max(prod_daily_instances.is_dagit_started) as is_dagit_started,
    max(prod_daily_instances.is_gql_query_completed) as is_gql_query_completed,
    max(prod_daily_instances.is_linux) as is_linux,
    max(prod_daily_instances.has_run_storage_id) as has_run_storage_id

from prod_daily_instances
inner join {{ ref('telemetry_run_instance_mapping') }} using (instance_id)
group by all
