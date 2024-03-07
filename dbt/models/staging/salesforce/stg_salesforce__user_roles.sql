select

    id as user_role_id,
    developername as developer_role_name,
    name as role_name

from {{ source('salesforce', 'userrole') }}
