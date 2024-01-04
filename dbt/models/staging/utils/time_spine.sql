{{ config( materialized='table') }}

with date_spine as (

  {{ dbt_utils.date_spine(
    start_date="cast('2020-01-01' as date)",
      datepart="day",
      end_date="dateadd(year, 15, current_date)"
     )
  }}

),

calculated as (

    select
        date_day,
        date_day as ds,
        date_day - interval '1 day' as prior_date_day,
        date_day + interval '1 day' as next_date_day,

        dayname(date_day) as day_name,


        date_part('month', date_day) as month_actual,
        date_part(quarter, date_day) as quarter_actual,
        date_part('year', date_day) as year_actual,

        date_part(dayofweek, date_day) + 1 as day_of_week,
        case
            when day_name = 'sun' then date_day
            else dateadd('day', -1, date_trunc('week', date_day))
        end as first_day_of_week,

        date_part('day', date_day) as day_of_month,

        row_number()
            over (partition by year_actual, quarter_actual order by date_day)
            as day_of_quarter,
        row_number() over (partition by year_actual order by date_day) as day_of_year,

        to_char(date_day, 'mmmm') as month_name,

        trunc(date_day, 'month') as first_day_of_month,
        last_value(date_day)
            over (partition by year_actual, month_actual order by date_day)
            as last_day_of_month,

        first_value(date_day)
            over (partition by year_actual order by date_day)
            as first_day_of_year,
        last_value(date_day) over (partition by year_actual order by date_day) as last_day_of_year,

        first_value(date_day)
            over (partition by year_actual, quarter_actual order by date_day)
            as first_day_of_quarter,
        last_value(date_day)
            over (partition by year_actual, quarter_actual order by date_day)
            as last_day_of_quarter,


        last_value(date_day)
            over (partition by first_day_of_week order by date_day)
            as last_day_of_week,

        (year_actual || '-q' || extract(quarter from date_day)) as quarter_name


    from date_spine

),

final as (

    select
        ds,
        date_day,
        prior_date_day,
        next_date_day,
        day_name,
        month_actual,
        year_actual,
        quarter_actual,
        day_of_week,
        first_day_of_week,
        day_of_month,
        day_of_quarter,
        day_of_year,
        month_name,
        first_day_of_month,
        last_day_of_month,
        first_day_of_year,
        last_day_of_year,
        first_day_of_quarter,
        last_day_of_quarter,
        last_day_of_week,
        quarter_name
    from calculated

)

select * from final
