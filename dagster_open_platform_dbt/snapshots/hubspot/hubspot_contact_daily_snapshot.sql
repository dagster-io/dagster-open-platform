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
        /*
            This will generate a new snapshot on the first run of the day
            in America/Los_Angeles timezone. We subtract 1 day because it
            represents the ending values of the previous day.
        */
        contact_id,
        email,
        email_domain,
        first_name,
        last_name,
        associatedcompanyid,
        organization_id,
        company,
        lead_source,
        trial_source,
        lifecycle_stage,
        lifecycle_stage_date,
        time_in_mql,
        lifecycle_stage_mql_date,
        predictive_scoring_tier,
        mql_date,
        is_mql,
        mql_status,
        predictive_contact_score,
        salesforce_contact_id,
        create_time,
        create_date
    from {{ ref('stg_hubspot__contacts') }}

{% endsnapshot %}