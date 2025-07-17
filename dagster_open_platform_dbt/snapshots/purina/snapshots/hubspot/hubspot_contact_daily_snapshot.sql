{% snapshot hubspot_contact_daily_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='contact_id',
      strategy='timestamp',
      updated_at='snapshot_date',
    )
}}

    select
        convert_timezone('UTC', 'America/Los_Angeles', current_timestamp)::date
        - interval '1 day' as snapshot_date,
        current_timestamp as snapshot_at,
        /*
            This will generate a new snapshot on the first run of the day
            in America/Los_Angeles timezone. We subtract 1 day because it
            represents the ending values of the previous day.
        */
        c1.*,
        c2.abm_score
    from {{ ref('stg_hubspot__contacts') }} c1
        left join {{ ref('abm_contact_intent_scores') }} c2 on c1.contact_id = c2.hubspot_contact_id

{% endsnapshot %}