select
    reporting_date,
    instance_id,
    case when
        not is_linux
        or (
            instance_id not in (select instance_id from {{ ref('telemetry_prod_instances') }})
            and left(dagster_version, 1) >= 1

            and (
                num_steps_started >= 1
                or num_runs_created_by_sensor >= 1
                or num_runs_created_by_schedule >= 1
                or num_runs_created_by_backfill >= 1
                or num_runs_launched_from_dagit >= 1
            )
        )
        then 1
    else 0 end as is_active

from {{ ref('telemetry_local_daily_instances') }}
order by 1
