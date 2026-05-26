{% snapshot salesforce_opportunities_daily_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='opportunity_id',
      strategy='timestamp',
      updated_at='snapshot_date',
    )
}}

    with opportunities as (

        select * from {{ ref('stg_salesforce__opportunities') }}
    ),

    line_items as (

        select
            opportunity_id,
            sum(annualized_arr) as total_annualized_arr,
            sum(iff(pro_rate_for_arr = true, 1, 0)) as prorated_product_count,
            sum(iff(pro_rate_for_arr = false, 1, 0)) as non_prorated_product_count
        from {{ ref("stg_salesforce__opportunity_line_item") }}
        group by 1
    ),

    opportunities_with_new_arr as (

        select
            opportunities.*,
            {{ salesforce_opportunity_new_arr(
                'opportunities.opportunity_type',
                'coalesce(line_items.prorated_product_count, 0)',
                'coalesce(line_items.non_prorated_product_count, 0)',
                'coalesce(line_items.total_annualized_arr, 0)',
                'opportunities.amount_year1',
                'opportunities.prior_term_arr',
                'opportunities.term_months',
            ) }} as new_arr
        from opportunities
        left join line_items on opportunities.opportunity_id = line_items.opportunity_id
    )

    select
        convert_timezone('UTC', 'America/Los_Angeles', current_timestamp)::date
        - interval '1 day' as snapshot_date,
        current_timestamp as snapshot_at,
        /*
            This will generate a new snapshot on the first run of the day
            in America/Los_Angeles timezone. We subtract 1 day because it
            represents the ending values of the previous day.
        */
        *
    from opportunities_with_new_arr

{% endsnapshot %}
