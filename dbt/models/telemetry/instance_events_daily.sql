{% set actions_dict = {
    "action = 'step_start_event'" : "step_started",
    "action = 'sensor_run_created'" : "sensor_run_created",
    "action = 'scheduled_run_created'" : "scheduled_run_created",
    "action = 'backfill_run_created'": "backfill_run_created",
    "action = 'daemon_alive'" : "daemon_alive",
    "action = 'start_dagit_webserver'" : "webserver_started",
    "action = 'graphql_query_completed'" : "gql_query_completed",
    """
    action = 'graphql_query_completed' and 
    metadata:operationName = 'PipelineRunsRootQuery' or 
    action = 'LAUNCH_RUN'
    """: "run_launched_from_webserver",
} -%}

with

instance_metrics as (
    select
        reporting_date,
        instance_id,
        min(instance_priority) as instance_priority,
        max(dagster_version_raw) as dagster_version_raw,
        max(dagster_version_parsed) as dagster_version_parsed,
        {% for action_condition, action_name in actions_dict.items() %}
            sum(case when {{ action_condition }} then 1 else 0 end)
                as {{ action_name }}_count,
        {% endfor %}
        (
            step_started_count >= 10
            or sensor_run_created_count >= 1
            or scheduled_run_created_count >= 1
            or backfill_run_created_count >= 1
        ) as is_active
    from {{ ref('events') }}
    group by all
)

select
    reporting_date,
    instance_id,
    instance_priority,
    instance_type,
    dagster_version_raw,
    dagster_version_parsed,
    {% for action_condition, action_name in actions_dict.items() %}
        {{ action_name }}_count,
    {% endfor %}
    is_active
from instance_metrics
inner join {{ ref('instance_type_priority') }} using (instance_priority)
