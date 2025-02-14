{% snapshot hubspot_company_daily_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='snapshot_date',
    )
}}

    select
        convert_timezone('UTC', 'America/Los_Angeles', current_timestamp)::date
        - interval '1 day' as snapshot_date,
        /*
            This will generate a new snapshot on the first run of the day
            in America/Los_Angeles timezone. We subtract 1 day because it
            represents the ending values of the previous day.
        */
        c1.*,
        c2.intent_score
    from {{ ref('stg_hubspot__company') }} c1
        left join {{ ref('abm_company_intent_scores') }} c2 on c1.id = c2.hubspot_company_id

{% endsnapshot %}