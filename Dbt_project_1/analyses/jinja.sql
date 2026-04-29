{% set variable = ['value1', 'value2']%}
{% set cols_lis = ['sales_id','customer_sk','unit_price']%}

select
{% for i in cols_lis -%}
    {{ i }}{{ "," if not loop.last }}
{% endfor %}
from {{ ref('raw_sales') }}

