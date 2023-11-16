with daily_data as (
    select
        run_storage_id,
        ds,
        is_active,
        sum(case when is_active = 0 then 1 else 0 end)
            over (partition by run_storage_id order by ds rows unbounded preceding)
            as grp
    from {{ ref('telemetry_prod_active_flag') }}
),

prod_active_counts as (
    select
        run_storage_id,
        ds,
        is_active,
        case
            when
                is_active = 1
                then row_number() over (partition by run_storage_id, grp order by ds) - 1
            else 0
        end as active_days_count
    from daily_data
    order by run_storage_id, ds


)


select distinct run_storage_id
from prod_active_counts
where active_days_count >= 28
