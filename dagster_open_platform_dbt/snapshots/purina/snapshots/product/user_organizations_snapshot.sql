{% snapshot user_organizations_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='unique_key',
      strategy='check',
      check_cols=['organization_id', 'user_id', 'relationship_created_at'],
      hard_deletes='invalidate'
    )
}}

    select
        {{ dbt_utils.generate_surrogate_key([
            'organization_id',
            'user_id',
            'relationship_created_at'
        ]) }} as unique_key,
        organization_id,
        user_id,
        relationship_created_at
    from {{ ref('user_organizations') }}

{% endsnapshot %}