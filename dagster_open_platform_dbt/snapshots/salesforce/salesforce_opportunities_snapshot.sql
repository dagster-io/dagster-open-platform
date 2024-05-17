{% snapshot salesforce_opportunities_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='opportunity_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ ref('stg_salesforce__opportunities') }}

{% endsnapshot %}
