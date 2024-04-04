with

levels as (
    select * from {{ ref('permission_levels') }}
),

final as (

    select
        user_id,
        organization_id,
        max(max_role_level) as max_role_level,
        min(first_created_at) as first_created_at

    from {{ ref('user_deployment_permissions') }}
    group by all
)

select

    final.*,
    levels.role_name

from final
inner join levels on final.max_role_level = levels.role_level
