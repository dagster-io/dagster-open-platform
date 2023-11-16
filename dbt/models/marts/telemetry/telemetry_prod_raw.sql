select
    reporting_date,
    instance_id,
    run_storage_id,
    dag_version_parsed,
    os_platform,
    iff(is_known_ci_env = 'True', true, false) as is_known_ci_env,
    case when action = 'step_start_event' then 1 else 0 end as step_started,
    case when action = 'sensor_run_created' then 1 else 0 end as sensor_run_created,
    case when action = 'scheduled_run_created' then 1 else 0 end as schedule_run_created,
    case when action = 'backfill_run_created' then 1 else 0 end as backfill_run_created,
    case when action = 'daemon_alive' then 1 else 0 end as daemon_alive,
    case when action = 'start_dagit_webserver' then 1 else 0 end as dagit_started,
    case when action = 'GRAPHQL_QUERY_COMPLETED' then 1 else 0 end as gql_query_completed,
    case when
        action = 'GRAPHQL_QUERY_COMPLETED'
        and metadata:operationName = 'PipelineRunsRootQuery'
        then 1
    else 0 end as run_launched_from_dagit,
    case when os_platform in ('Linux') then 1 else 0 end as is_linux,
    case when run_storage_id != '' then 1 else 0 end as has_run_storage_id
from {{ ref('telemetry_prod_stg') }}
where
    true
    and reporting_date is not null
