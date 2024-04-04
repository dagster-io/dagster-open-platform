{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}

select
    step_data_id as id,
    organization_id,
    deployment_id,
    step_key,
    run_id,
    coalesce(repository_name, 'default') as repository_name,
    coalesce(code_location_name, 'default') as code_location_name,
    case
        when startswith(job_name, '__ASSET_JOB') then '__ASSET_JOB'
        else job_name
    end as pipeline_name,
    start_time as step_start_timestamp,
    end_time as step_end_timestamp,
    _incremented_at as last_rebuilt,
    run_ended_at,
    run_started_at

from {{ ref("fct_steps") }}
where
    start_time is not null
    and
    end_time is not null

    {% if is_incremental() %}
        and run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
    {% endif %}
