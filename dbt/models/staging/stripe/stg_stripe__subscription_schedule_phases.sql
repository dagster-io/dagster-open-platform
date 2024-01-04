select

    id as schedule_phase_id,
    schedule_id,
    start_date,
    end_date

from {{ source('stripe_pipeline', 'subscription_schedule_phases') }}
