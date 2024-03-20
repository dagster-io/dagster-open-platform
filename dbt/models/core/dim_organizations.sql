with sf_accounts as (
    select
        account_id,
        organization_id,
        account_type,
        account_status

    from {{ ref('stg_salesforce__accounts') }}
    where not is_deleted and organization_id regexp '\\d+'
    qualify row_number() over (partition by organization_id order by created_at desc) = 1
),

stripe_customer as (
    select

        organization_id,
        stripe_customer_id

    from {{ ref('stg_postgres__customer_info') }}
),

orgs as (
    select *
    from {{ ref('stg_postgres__organizations') }}
),

user_orgs as (
    select

        organization_id,
        max(last_user_login) as last_user_login

    from {{ ref('user_organizations') }}
    where not is_elementl_user
    group by 1
),

runs as (
    select

        organization_id,
        max(started_at) as last_run_at

    from {{ ref('stg_postgres__runs') }}
    group by 1
),

final as (

    select

        orgs.organization_id,
        orgs.organization_name,

        orgs.plan_type,
        orgs.is_internal,
        orgs.has_saml_sso,

        coalesce(orgs.status, 'ACTIVE') as status,
        coalesce(orgs.status, 'ACTIVE') = 'ACTIVE' and not orgs.is_internal as is_active,

        user_orgs.last_user_login,
        runs.last_run_at,

        sf_accounts.account_id as salesforce_account_id,
        coalesce(sf_accounts.account_type, 'unset') as salesforce_account_type,
        coalesce(sf_accounts.account_status, 'unset') as salesforce_account_status,

        stripe_customer.stripe_customer_id,

        orgs.organization_metadata,
        orgs.organization_settings,

        orgs.created_at as org_created_at,
        orgs.updated_at as org_updated_at

    from orgs
    left join user_orgs using (organization_id)
    left join stripe_customer using (organization_id)
    left join runs using (organization_id)
    left join sf_accounts using (organization_id)
)

select * from final
