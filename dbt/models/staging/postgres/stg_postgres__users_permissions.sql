select

    id as user_permission_id,
    permission_id,
    organization_id,
    user_id,
    create_timestamp as created_at

from {{ source("cloud_product", "users_permissions") }}
