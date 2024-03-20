with user_permissions as (
    select * from {{ ref('stg_postgres__users_permissions') }}
),

permissions as (
    select * from {{ ref('stg_postgres__permissions') }}
),

deployments as (
    select * from {{ ref('stg_postgres__deployments') }}
),

levels as (
    select * from {{ ref('permission_levels') }}
),

users as (
    select * from {{ ref('stg_postgres__users') }}
),

user_deployment_permissions as (

    select

        user_permissions.organization_id,
        permissions.deployment_id,
        user_permissions.user_id,
        users.email,
        user_permissions.created_at,
        permissions.scope,
        levels.role_level,
        levels.role_name

    from user_permissions
    inner join permissions using (permission_id)
    inner join users using (user_id)
    inner join levels on permissions.permission_grant = levels.role_name
    where users.is_elementl_user = false
),

org_admins as (
    select
        udp.organization_id,
        d.deployment_id,
        udp.user_id,
        udp.email,
        udp.created_at,
        udp.scope,
        udp.role_level,
        udp.role_name
    from user_deployment_permissions as udp
    inner join deployments as d using (organization_id)
    where
        udp.deployment_id is null
        and (scope = 'ORGANIZATION' or scope is null)
        and role_name = 'ADMIN'
-- this filter ensures we're only grabbing user permissions 
-- where they've explicitly been designated an org admin,
-- deployment will always be null, scope can be null or 
-- ORGANIZATION and the role name must be ADMIN
),

non_org_admins as (
    select *
    from user_deployment_permissions
    where
        not (
            deployment_id is null
            and scope = 'ORGANIZATION'
            and role_name = 'ADMIN'
        )
        and (scope != 'ALL_BRANCH_DEPLOYMENTS' or scope is null)
-- negate the org_admin filter above, and also remove branch 
-- deployment permissions since we don't care about them
),

all_deployment_permissions as (
    select * from org_admins
    union all
    select * from non_org_admins
)

select
    adp.organization_id,
    adp.deployment_id,
    adp.user_id,
    adp.created_at as first_created_at,
    adp.role_level as max_role_level,
    adp.role_name
from all_deployment_permissions as adp
inner join deployments using (deployment_id)
where deployments.is_active
qualify
    row_number()
        over (partition by organization_id, deployment_id, user_id order by created_at desc)
    = 1
