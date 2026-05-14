from dagster import ScheduleDefinition
from dagster_open_platform.defs.maxio.py.jobs import maxio_arr_correction

maxio_arr_correction_daily_schedule = ScheduleDefinition(
    job=maxio_arr_correction,
    cron_schedule="0 7 * * *",
    execution_timezone="America/New_York",
    description="Daily correction of Maxio ARR/MRR for contracted-value item types.",
)
