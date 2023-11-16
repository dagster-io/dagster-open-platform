with min_date as (
    select
        run_storage_id,
        min(reporting_date) as first_storage_date
    from {{ ref('telemetry_prod_daily_run_storage') }}
    group by 1
),

prod_date_spined as (
    select

        min_date.run_storage_id,
        min_date.first_storage_date,
        spine.ds

    from purina.utils.time_spine as spine
    inner join min_date
        on
            spine.ds >= min_date.first_storage_date
            and spine.ds < current_date
    order by 1, 3
),

prod_run_spined as (

    select

        prod_date_spined.ds,
        prod_date_spined.first_storage_date,
        prod_date_spined.run_storage_id,
        telemetry_prod_daily_run_storage.dagster_version,
        telemetry_prod_daily_run_storage.os_platform,
        coalesce(telemetry_prod_daily_run_storage.num_runs_created_by_sensor, 0)
            as num_runs_created_by_sensor,
        coalesce(telemetry_prod_daily_run_storage.num_runs_created_by_schedule, 0)
            as num_runs_created_by_schedule,
        coalesce(telemetry_prod_daily_run_storage.num_runs_created_by_backfill, 0)
            as num_runs_created_by_backfill,
        coalesce(telemetry_prod_daily_run_storage.num_runs_launched_from_dagit, 0)
            as num_runs_launched_from_dagit,
        coalesce(telemetry_prod_daily_run_storage.num_steps_started, 0) as num_steps_started,
        coalesce(telemetry_prod_daily_run_storage.is_daemon_live, 0) as is_daemon_live,
        coalesce(telemetry_prod_daily_run_storage.is_dagit_started, 0) as is_dagit_started,
        coalesce(telemetry_prod_daily_run_storage.is_gql_query_completed, 0)
            as is_gql_query_completed,
        coalesce(telemetry_prod_daily_run_storage.is_linux, 0) as is_linux,
        coalesce(telemetry_prod_daily_run_storage.has_run_storage_id, 0) as has_run_storage_id

    from prod_date_spined
    left join {{ ref('telemetry_prod_daily_run_storage') }} on
        prod_date_spined.ds = telemetry_prod_daily_run_storage.reporting_date
        and prod_date_spined.run_storage_id = telemetry_prod_daily_run_storage.run_storage_id
)




select
    ds,
    first_storage_date,
    run_storage_id,
    case when
        is_linux
        and (
            num_steps_started >= 10
            or num_runs_created_by_sensor >= 1
            or num_runs_created_by_schedule >= 1
            or num_runs_created_by_backfill >= 1
        )
        then 1
    else 0 end as is_active

from prod_run_spined
order by 1
