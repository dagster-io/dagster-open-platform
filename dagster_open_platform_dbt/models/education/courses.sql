select
    'thinkific' as source,
    course_id,
    name,
    slug,
    product_id,
    description,
    keywords,
    banner_image_url,
    course_card_image_url,
    user_id,
    reviews_enabled,
    instructor_id
from {{ ref('stg_thinkific__courses') }}
