select distinct

    id as user_permission_id,
    permission_id,
    organization_id,
    user_id,
    create_timestamp as created_at

from {{ source("postgres_etl_low_freq", "users_permissions") }}
