select
    to_timestamp(ds, 9) as ds,
    channels_count,
    full_members_count,
    claimed_full_members_count,
    readers_count_1d as daily_active_members,
    writers_count_1d as daily_active_posters,
    readers_count_7d as weekly_active_members,
    writers_count_7d as weekly_active_posters,
    messages_count
from {{ source('slack', 'member_metrics') }}
order by 1
