select
    id,
    title,
    review_text,
    course_id,
    rating,
    user_id,
    approved,
    created_at
from {{ source('thinkific', 'course_reviews') }}
