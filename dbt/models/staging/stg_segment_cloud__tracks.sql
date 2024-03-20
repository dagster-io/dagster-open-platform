select *

from {{ source('segment_app', 'tracks') }}
