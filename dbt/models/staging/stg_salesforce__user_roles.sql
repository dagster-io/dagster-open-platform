select

    id as user_role_id,
    developer_name as developer_role_name,
    name as role_name

from {{ source('salesforce', 'user_role') }}
