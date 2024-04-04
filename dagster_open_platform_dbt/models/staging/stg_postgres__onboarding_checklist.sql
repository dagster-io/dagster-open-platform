select
    {{ dbt_utils.generate_surrogate_key(['organization_id', 'entry_key']) }} as primary_id,
    organization_id,
    entry_key,
    status,
    create_timestamp as created_at,
    update_timestamp as updated_at

from {{ source("cloud_product", 'onboarding_checklist') }}
