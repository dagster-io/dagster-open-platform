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
        marketing_lead_source,
        latest_source,
        lead_source_action,
        latest_source_drill_level_1,
        latest_source_drill_level_2,
        latest_source_date,
        original_source,
        original_source_drill_level_1,
        original_source_drill_level_2,
        import_source,
        lifecycle_stage,
        lifecycle_stage_date,
        hubspot_score,
        time_in_mql,
        lifecycle_stage_mql_date,
        predictive_scoring_tier,
        mql_date,
        is_mql,
        mql_status,
        predictive_contact_score,
        salesforce_contact_id,
        create_time,
        create_date,
        request_type,
        how_did_you_hear_about_us,
        message,
        abm_score
    from {{ ref('stg_hubspot__contacts') }} c1
        left join {{ ref('abm_contact_intent_scores') }} c2 on c1.contact_id = c2.hubspot_contact_id

{% endsnapshot %}