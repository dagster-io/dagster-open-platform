with

events_model as (
    select
        lower(action) as action,
        event_id,
        elapsed_time,
        instance_id,
        metadata,
        python_version,
        dagster_version,
        regexp_substr(dagster_version, $$\d+\.\d+$$) as dagster_version_parsed,
        os_platform,
        run_storage_id,
        is_known_ci_env,
        reporting_date,
        metadata:client_id::string as client_id,
        metadata:location_name_hash::string as code_location_hash,
        case
            when (
                left(dagster_version_parsed, 1) >= 1
                and os_platform in ('Linux', 'FreeBSD')
                and not is_known_ci_env
            ) then 'SERVER'
            when (
                left(dagster_version_parsed, 1) >= 1
                and os_platform in ('Linux', 'FreeBSD')
                and is_known_ci_env
            ) then 'CI'
            when (
                left(dagster_version_parsed, 1) >= 1
                and os_platform not in ('Linux', 'FreeBSD')
                and not is_known_ci_env
            ) then 'LOCAL'
            else 'UNKNOWN'
        end as instance_type
    from {{ ref('stg_telemetry__events') }}
)

select
    action,
    event_id,
    elapsed_time,
    instance_id,
    metadata,
    python_version,
    dagster_version,
    dagster_version_parsed,
    os_platform,
    run_storage_id,
    is_known_ci_env,
    reporting_date,
    client_id,
    code_location_hash,
    instance_priority,
    instance_type
from events_model
inner join {{ ref('instance_type_priority') }} using (instance_type)
