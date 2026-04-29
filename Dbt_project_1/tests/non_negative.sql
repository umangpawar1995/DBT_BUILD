select
*
from {{ ref('raw_sales') }}
where gross_amount < 0 and net_amount < 0