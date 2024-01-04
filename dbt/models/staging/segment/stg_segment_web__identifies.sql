select

    id as event_id,
    anonymous_id,
    timestamp,
    * ilike 'CLEARBIT%' -- noqa

from {{ source('segment_web', 'identifies') }}
