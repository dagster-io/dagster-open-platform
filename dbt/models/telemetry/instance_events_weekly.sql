{% set metrics_list = [
    "step_started",
    "sensor_run_created",
    "scheduled_run_created",
    "backfill_run_created",
    "daemon_alive",
    "webserver_started",
    "gql_query_completed",
    "run_launched_from_webserver",
] -%}

with

instance_metrics as (
    select
        date_trunc(week, reporting_date) as reporting_week,
        instance_id,
        min(instance_priority) as instance_priority,
        max(dagster_version_raw) as dagster_version_raw,
        max(dagster_version_parsed) as dagster_version_parsed,
        {% for metric in metrics_list %}
            sum({{ metric }}_count) as {{ metric }}_count,
        {% endfor %}
        count_if(is_active) as active_days_count
    from {{ ref('instance_events_daily') }}
    group by all
)

select
    reporting_week,
    instance_id,
    instance_priority,
    instance_type,
    dagster_version_raw,
    dagster_version_parsed,
    {% for metric in metrics_list %}
        {{ metric }}_count,
    {% endfor %}
    active_days_count >= 5 and instance_type = 'SERVER' as is_production
from instance_metrics
inner join {{ ref('instance_type_priority') }} using (instance_priority)
