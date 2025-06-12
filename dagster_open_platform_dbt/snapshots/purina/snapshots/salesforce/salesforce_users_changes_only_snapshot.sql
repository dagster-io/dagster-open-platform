{% snapshot salesforce_users_changes_only_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='check',
      check_cols=[
        'email',
        'first_name', 
        'last_name',
        'name',
        'is_active',
        'user_role_id',
        'role_custom_field',
        'user_name',
        'region',
        'in_rotation',
        'sales_start_date',
        'sales_end_date',
        'assign_bdr_id'
      ],
    )
}}

    select
        current_timestamp as snapshot_at,
        /*
            This snapshot captures changes when any monitored column changes.
            The snapshot_date represents when the snapshot was taken.
        */
        *
    from {{ ref('salesforce_users') }}

{% endsnapshot %}
