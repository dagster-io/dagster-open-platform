select
    'thinkific' as source,
    enrollment_id,
    created_at,
    user_email,
    user_name,
    user_id,
    course_name,
    course_id,
    percentage_completed,
    expired,
    is_free_trial,
    completed,
    started_at,
    activated_at,
    updated_at,
    completed_at
from {{ ref('stg_thinkific__enrollments') }}
