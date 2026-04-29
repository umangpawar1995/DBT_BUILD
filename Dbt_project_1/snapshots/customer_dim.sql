{% snapshot customer_dim %}

{{
    config(
        target_schema = 'core',
        unique_key = 'customer_id',
        strategy = 'timestamp',
        updated_at = 'updated_at'
    )
}}

select * from {{ ref('stg_customer') }}

{% endsnapshot %}