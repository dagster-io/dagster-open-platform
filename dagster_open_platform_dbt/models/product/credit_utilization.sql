with

organizations as (
    select * from {{ ref('dim_organizations') }}
),

sfdc_accounts as (
    select * from {{ ref('stg_salesforce__accounts') }}
),

product_usage as (
    select * from {{ ref('usage_metrics_daily') }}
),

time_spine as (
    select * from {{ ref('time_spine') }}
),

contracts as (
    select * from {{ ref('stg_salesforce__contracts') }}
),

subs as (
    /* We want the most recent subscription for all enterprise customers, including those
    who have churned. As a result, we filter for the latest subscription, not the active
    one */
    select

        organizations.organization_id,
        organizations.organization_name,
        account_name,
        account_status,
        arr,
        cloud_credits_contracted,
        contract_term,
        contract_start_date,
        contract_end_date,
        datediff('days', contract_start_date, contract_end_date) as contract_length,
        launcher_seats,
        pricing_model,
        contract_status,
        row_number() over (
            partition by account_id order by contract_start_date desc
        ) as subscription_index

    from contracts
    inner join sfdc_accounts using (account_id)
    inner join organizations
        on
            sfdc_accounts.organization_id = organizations.organization_id
            and contract_start_date < current_date()
    qualify subscription_index = 1
    order by organizations.organization_id, subscription_index
),

orgs as (
    select

        organization_id,
        organization_name,
        status,
        org_created_at

    from organizations
    where
        plan_type = 'ENTERPRISE'
        and not is_internal
),

daily_usage as (
    select
        time_spine.ds,
        subs.organization_id,
        subs.contract_start_date,
        subs.contract_end_date,
        subs.cloud_credits_contracted,
        sum(product_usage.materializations + product_usage.steps_credits) as total_credits

    from subs
    full join time_spine on time_spine.ds between contract_start_date and contract_end_date
    left join product_usage
        on
            time_spine.ds = product_usage.ds
            and subs.organization_id = product_usage.organization_id

    group by all
),

weekly_usage as (
    select
        date_trunc(week, ds) as week_start_date,
        organization_id,
        datediff(week, contract_start_date, contract_end_date) as contract_term_weeks,
        cloud_credits_contracted / contract_term_weeks as weekly_allotted_credits,
        sum(total_credits) as week_total_credits,
        zeroifnull(div0(week_total_credits, weekly_allotted_credits)) as week_utilization_pct
    from daily_usage
    group by all
),

combined as (
    select

        orgs.organization_id,
        orgs.organization_name,
        orgs.status as organization_status,
        orgs.org_created_at,
        subs.cloud_credits_contracted as annual_cloud_credits_contract,
        subs.contract_start_date,
        subs.contract_end_date,
        subs.contract_term,
        subs.contract_length as contract_length_days,
        subs.pricing_model,
        subs.launcher_seats,
        iff(subs.organization_id is null, true, false) as missing_subscription,
        iff(daily_usage.organization_id is null, true, false) as missing_usage,
        iff(subs.cloud_credits_contracted is null, true, false) as missing_credits,
        daily_usage.ds as credits_date,
        coalesce(daily_usage.total_credits, 0) as total_credits_used,
        week_utilization_pct

    from orgs
    left join subs using (organization_id)
    left join daily_usage
        on
            orgs.organization_id = daily_usage.organization_id
            and daily_usage.ds between subs.contract_start_date and subs.contract_end_date
    left join weekly_usage
        on
            orgs.organization_id = weekly_usage.organization_id
            and date_trunc(week, daily_usage.ds) = weekly_usage.week_start_date


),

features as (

    select
        combined.*,
        datediff('days', org_created_at, credits_date) as organization_age_at_usage_day,
        datediff('days', contract_start_date, credits_date) as contract_age_at_usage_day,
        datediff('months', org_created_at, credits_date) as organization_age_at_usage_month,
        datediff('months', contract_start_date, credits_date) as contract_age_at_usage_month,
        datediff('months', contract_start_date, current_date()) as contract_age_today_month,
        annual_cloud_credits_contract / contract_length_days as daily_cloud_credits_contracted
    from combined
),

final as (
    select
        *,
        sum(total_credits_used)
            over (partition by organization_id order by credits_date)
            as cume_credits_used,
        sum(daily_cloud_credits_contracted)
            over (partition by organization_id order by credits_date)
            as cume_cloud_credits,
        cume_credits_used / cume_cloud_credits as cloud_credit_utilization

    from features
    order by organization_id, credits_date
)

select * from final
