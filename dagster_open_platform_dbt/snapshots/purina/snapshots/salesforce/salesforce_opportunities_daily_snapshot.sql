{% snapshot salesforce_opportunities_daily_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='opportunity_id',
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
        *
    from {{ ref('stg_salesforce__opportunities') }}

{% endsnapshot %}
