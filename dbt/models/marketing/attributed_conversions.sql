with user_org_permissions as (
    select * from {{ ref('user_org_permissions') }}
),

first_touch as (
    select * from {{ ref('first_touch') }}
),

organizations as (
    select * from {{ ref('dim_organizations') }}
),

users as (
    select * from {{ ref('dim_users') }}
),

workspace_creating_user as (
    select

        organization_id,
        user_id::string as user_id,
        first_created_at,
        row_number() over (partition by organization_id order by first_created_at) as rn

    from user_org_permissions

    qualify rn = 1
),

first_touch_cats as (
    select

        blended_user_id,
        session_id,
        session_started_at,
        attribution_category

    from first_touch

    qualify row_number() over (partition by blended_user_id order by session_started_at asc) = 1
),

final as (
    select

        users.user_id,
        users.user_name,
        users.email,
        users.user_title,
        users.experience,
        users.created_at as user_created_at,

        workspace_creating_user.first_created_at as user_workspace_first_created_at,

        organizations.organization_id,
        organizations.organization_name,
        organizations.org_created_at,
        organizations.is_active,
        organizations.plan_type,
        organizations.status as organisation_status,
        organizations.last_run_at,

        first_touch_cats.session_id,
        first_touch_cats.session_started_at,
        first_touch_cats.attribution_category

    from workspace_creating_user
    inner join first_touch_cats
        on
            workspace_creating_user.user_id = first_touch_cats.blended_user_id
            and workspace_creating_user.first_created_at >= first_touch_cats.session_started_at
            and datediff(
                'days',
                first_touch_cats.session_started_at,
                workspace_creating_user.first_created_at
            )
            <= {{ var('attribution_lookback_days') }}
    inner join organizations using (organization_id)
    inner join users using (user_id)
    where not organizations.is_internal
)

select * from final
