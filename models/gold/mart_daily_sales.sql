select
    dd.full_date as order_day,
    coalesce(fo.order_channel, 'Unknown') as order_channel,
    coalesce(fo.customer_segment_at_time, 'Unknown') as customer_segment_at_time,
    count(*) as total_orders,
    count(distinct fo.customer_key) as unique_customers,
    sum(fo.order_value) as total_revenue,
    avg(fo.order_value) as avg_order_value,
    coalesce(sum(fo.order_value) filter (where fo.attributed_to_promo), 0) as promo_attributed_revenue,
    count(*) filter (where fo.attributed_to_promo) as promo_attributed_orders
from {{ ref('fact_orders') }} fo
join {{ ref('dim_date') }} dd
    on fo.order_date_key = dd.date_key
where fo.order_status = 'PAID'
  and fo.order_date_key <> -1
group by
    dd.full_date,
    coalesce(fo.order_channel, 'Unknown'),
    coalesce(fo.customer_segment_at_time, 'Unknown')
