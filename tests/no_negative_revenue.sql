select 'fact_orders' as table_name, order_id::text as record_id, order_value as invalid_amount
from {{ ref('fact_orders') }}
where order_value < 0

union all

select 'fact_order_items', order_item_id::text, line_total
from {{ ref('fact_order_items') }}
where line_total < 0
