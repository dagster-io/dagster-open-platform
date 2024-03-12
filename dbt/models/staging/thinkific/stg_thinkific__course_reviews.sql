select
    id as course_review_id,
    title,
    review_text,
    course_id,
    rating,
    user_id,
    approved,
    created_at
from {{ source('thinkific', 'course_reviews') }}
