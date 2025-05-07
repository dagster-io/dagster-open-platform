{% snapshot deployments_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='deployment_id',
      strategy='timestamp',
      updated_at='snapshot_date',
    )
}}

    select
        convert_timezone('UTC', 'America/Los_Angeles', current_timestamp)::date
        - interval '1 day' as snapshot_date,
        current_timestamp as snapshot_at,
        *
    from {{ ref('stg_cloud_product__deployments') }}

{% endsnapshot %} 