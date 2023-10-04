select
    {{ dbt_utils.surrogate_key(['organization_id', 'entry_key']) }} as primary_id,
    organization_id,
    entry_key,
    status,
    create_timestamp as created_at,
    update_timestamp as updated_at

from {{ source('postgres_etl_low_freq', 'onboarding_checklist') }}
