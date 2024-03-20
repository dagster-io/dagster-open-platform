{{ config( materialized='table') }}
with subscriptions as (
    select * from {{ ref('stg_stripe__subscriptions') }}
),

schedules as (
    select * from {{ ref('stg_stripe__subscription_schedules') }}
),

phases as (
    select * from {{ ref('stg_stripe__subscription_schedule_phases') }}
),

sub_metadata as (
    select
        subscription_id,
        max(iff(key = 'plan', value, null)) as plan,
        max(iff(key = 'Cloud_Credits', value, null)) as cloud_credits
    from {{ ref('stg_stripe__subscriptions_metadata') }}
    where key in ('Cloud_Credits', 'plan')
    group by 1
),

final as (

    select

        subscriptions.customer_id,
        subscriptions.subscription_id,
        subscriptions.status as subscription_status,
        subscriptions.created_at,
        subscriptions.trial_start,
        subscriptions.trial_end,
        sub_metadata.plan as subscription_metadata_plan,
        sub_metadata.cloud_credits as cloud_credits_contract,
        phases.start_date as contract_start_date,
        phases.end_date as contract_end_date,
        datediff('days', phases.start_date, phases.end_date) as contract_length_days,
        dense_rank()
            over (partition by subscriptions.customer_id order by subscriptions.created_at desc)
            as sub_idx

    from subscriptions
    left join schedules using (subscription_id)
    left join phases using (schedule_id)
    left join sub_metadata using (subscription_id)
)

select * from final
