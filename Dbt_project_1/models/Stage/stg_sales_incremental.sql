{{
      config(
        materialized = 'incremental',
        unique_key = 'sales_id'
        )
}}

with sales as (

select

    sales_id,

    product_sk,

    unit_price,

    gross_amount,

    {{ multiply('unit_price', 'gross_amount') }} as total_price,

    discount_amount

from {{ ref('raw_sales') }}

{% if is_incremental()%}
    where sales_id > (select max(sales_id) from {{ this }})
{% endif %}

),

products as (

select

    product_sk,

    product_name,

    category

from {{ ref('raw_product') }}

),

joined as (

select

    s.sales_id,

    s.product_sk,

    s.unit_price,

    s.gross_amount,

    s.total_price,

    s.discount_amount,

    p.product_name,

    p.category

from sales s

join products p using (product_sk)

)

select *,

current_timestamp() as refresh_date

from joined