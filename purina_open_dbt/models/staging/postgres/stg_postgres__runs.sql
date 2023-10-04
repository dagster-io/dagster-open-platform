select

    run_id,
    id as row_id,

    organization_id,
    snapshot_id,
    deployment_id,

    pipeline_name as job_name,
    repository_label,
    split_part(repository_label, '@', 1) as repository_name,
    split_part(repository_label, '@', 2) as code_location_name,
    status,
    mode,
    start_time as started_at,
    end_time as ended_at,

    create_timestamp as created_at,
    update_timestamp as updated_at

from {{ source('postgres_etl_high_freq', 'runs') }}
where
    {{ limit_dates_for_dev(ref_date = 'create_timestamp') }}
/* At least one duplicate run_id was identified, so we pick the latest
entry using the primary_id column */
qualify row_number() over (partition by run_id order by row_id desc) = 1
