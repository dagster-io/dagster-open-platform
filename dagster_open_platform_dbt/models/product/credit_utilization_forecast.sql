with credit_utilization as (
    select * from {{ ref('credit_utilization') }}
),

forecast as (
    select distinct

        organization_id,
        organization_name,
        organization_status,
        org_created_at,
        contract_start_date,
        contract_end_date,
        contract_term,
        contract_length_days,
        pricing_model,
        launcher_seats,
        annual_cloud_credits_contract,

        regr_intercept(cume_credits_used, contract_age_at_usage_day)
            over (partition by organization_id) as intercept,
        regr_slope(cume_credits_used, contract_age_at_usage_day)
            over (partition by organization_id) as slope,
        regr_r2(cume_credits_used, contract_age_at_usage_day)
            over (partition by organization_id) as r2,
        last_value(cume_credits_used)
            over (
                partition by organization_id order by credits_date range
                between unbounded preceding and unbounded following
            ) as cume_credits_used,
        last_value(cloud_credit_utilization)
            over (
                partition by organization_id order by credits_date range
                between unbounded preceding and unbounded following
            ) as utilization,
        zeroifnull(
            nth_value(week_utilization_pct, 7) -- Get the previous full week's utilization
                over (
                    partition by organization_id order by credits_date desc
                )
        ) as last_week_utilization_pct

    from credit_utilization
    where
        cloud_credit_utilization is not null
        and credits_date <= current_date()
)

select
    forecast.*,
    greatest(slope * contract_length_days + intercept, cume_credits_used)::number
        as predicted_cume_credits_used,
    predicted_cume_credits_used / annual_cloud_credits_contract as predicted_utilization

from forecast
