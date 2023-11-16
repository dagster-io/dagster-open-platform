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
from {{ ref('telemetry_local_raw') }}
where not is_known_ci_env
group by 1, 2
