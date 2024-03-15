select
    id as user_id,
    email,
    first_name,
    last_name,
    name,
    is_active,
    user_role_id

from {{ source('salesforce', 'user') }}
