with session_tokens as (
    select * from {{ ref('stg_postgres__session_tokens') }}
),

users as (
    select * from {{ ref('stg_postgres__users') }}
),

user_orgs as (
    select * from {{ ref('stg_postgres__user_organizations') }}
),

user_sessions as (

    select

        session_tokens.organization_id,
        max(users.created_at) as last_user_login

    from session_tokens
    inner join users on session_tokens.user_id = users.user_id

    where not users.is_elementl_user
    group by 1
),

final as (

    select


        user_orgs.organization_id,
        user_orgs.user_id,
        users.email,
        users.is_elementl_user,
        user_sessions.last_user_login

    from user_orgs
    left join users using (user_id)
    left join user_sessions using (organization_id)

)

select * from final
