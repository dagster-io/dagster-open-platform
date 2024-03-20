with

organizations as (
    select
        organization_id,
        organization_name,
        salesforce_account_id,
        is_active as is_active_org
    from {{ ref('dim_organizations') }}
    where salesforce_account_id is not null
),

organization_deployment_count as (
    select
        organization_id,
        date(date_trunc(month, deployment_created_day)) as month_start_date,
        max(total_deployments) as deployment_count,
        max(total_active_deployments) as active_deployment_count,
        max(total_branch_deployments) as branch_deployment_count,
        max(total_hybrid_deployments) as hybrid_deployment_count
    from {{ ref('org_deployments_daily') }}
    group by organization_id, month_start_date
),

monthly_credit_usage as (
    select
        date(date_trunc(month, ds)) as month_start_date,
        organization_id,
        sum(materializations) as materializations_credits,
        sum(steps_credits) as steps_credits,
        materializations_credits + sum(steps_credits) as dagster_credits
    from {{ ref('usage_metrics_daily') }}
    group by month_start_date, organization_id
)

select
    month_start_date,
    organization_id,
    {{ dbt_utils.generate_surrogate_key(["month_start_date", "salesforce_account_id"]) }}
        as surrogate_key,
    organization_name,
    salesforce_account_id,
    is_active_org,
    coalesce(
        deployment_count,
        lag(deployment_count) ignore nulls
            over (partition by organization_id order by month_start_date)
    ) as deployment_count,
    coalesce(
        active_deployment_count,
        lag(active_deployment_count) ignore nulls
            over (partition by organization_id order by month_start_date)
    ) as active_deployment_count,
    coalesce(
        branch_deployment_count,
        lag(branch_deployment_count) ignore nulls
            over (partition by organization_id order by month_start_date)
    ) as branch_deployment_count,
    coalesce(
        hybrid_deployment_count,
        lag(hybrid_deployment_count) ignore nulls
            over (partition by organization_id order by month_start_date)
    ) as hybrid_deployment_count,
    zeroifnull(materializations_credits) as materializations_credits,
    zeroifnull(steps_credits) as steps_credits,
    zeroifnull(dagster_credits) as dagster_credits
from monthly_credit_usage
full outer join organization_deployment_count using (organization_id, month_start_date)
inner join organizations using (organization_id)
order by organization_id, month_start_date
