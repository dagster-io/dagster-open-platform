select
    'thinkific' as source,
    course_review_id,
    title,
    review_text,
    course_id,
    rating,
    user_id,
    approved,
    created_at
from {{ ref('stg_thinkific__course_reviews') }}
