{{
    config(
        materialization = 'incremental',
        unique_key = 'customer_id'
    )
}}

{% set columns = ['customer_id','first_name','last_name','email','city','updated_at'] %}
with source as (
    select
        {% for i in columns %}
            {{ i }}  {% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ ref('raw_customer')}}

    {% if is_incremental()%}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),
dedup as (
    select
        *,
        row_number() over (partition by customer_id order by updated_at desc) as rn
    from source
),
cleaned as (
    select
        customer_id,
        upper(first_name) as first_name,
        upper(last_name) as last_name,
        coalesce(email, 'unknown@gmail.com') as email,
        coalesce(city, 'UNKNOWN') as city,
        updated_at
    from dedup
    where rn = 1
)
select * from cleaned