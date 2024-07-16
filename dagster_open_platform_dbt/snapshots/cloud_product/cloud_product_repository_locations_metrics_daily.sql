{% snapshot cloud_product_repository_metrics_daily %}

{{
    config(
      target_schema='snapshots',
      unique_key="organization_id||'-'||deployment_id||'-'||code_location",
      strategy='timestamp',
      updated_at='timestamp'
    )
}}

select current_timestamp() as timestamp, * from {{ ref('repository_locations_metrics') }}

{% endsnapshot %}
