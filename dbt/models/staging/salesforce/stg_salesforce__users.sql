select
    id as user_id,
    email,
    firstname as first_name,
    lastname as last_name,
    name as full_name,
    isactive as is_active,
    userroleid as user_role_id

from {{ source('salesforce', 'user') }}
