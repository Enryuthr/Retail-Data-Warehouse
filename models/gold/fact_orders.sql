select
    o.order_id,
    coalesce(dc.customer_key, -1) as customer_key,
    coalesce(dp.promotion_key, -1) as promotion_key,
    coalesce(to_char(o.order_date::date, 'YYYYMMDD')::integer, -1) as order_date_key,
    o.order_date,
    o.order_status,
    coalesce(o.order_channel, 'Unknown') as order_channel,
    o.order_value,
    o.attributed_to_promo,
    coalesce(o.customer_segment_at_time, 'Unknown') as customer_segment_at_time
from {{ ref('orders') }} o
left join {{ ref('dim_customer') }} dc
    on o.customer_id = dc.customer_id
left join {{ ref('dim_promotion') }} dp
    on o.promotion_id = dp.promotion_id
