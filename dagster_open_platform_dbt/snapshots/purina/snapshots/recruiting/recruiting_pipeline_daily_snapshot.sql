{% snapshot recruiting_pipeline_daily_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key="job_id || '-' || stage_id",
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
            
            Only snapshots open jobs to track daily pipeline changes.
        */
        pipeline.*
    from {{ ref('recruiting_pipeline') }} pipeline
    where pipeline.is_active_job = true

{% endsnapshot %}
