select
    coalesce(oi.product_category, 'Unknown') as product_category,
    coalesce(sum(oi.quantity), 0) as total_items_sold,
    coalesce(sum(oi.item_revenue), 0) as total_revenue,
    count(distinct oi.order_id) as unique_orders,
    count(distinct o.customer_id) as unique_customers,
    coalesce(sum(oi.item_revenue) filter (where oi.attributed_to_promo), 0) as promo_attributed_revenue
from {{ ref('order_items') }} oi
join {{ ref('orders') }} o
    on oi.order_id = o.order_id
where o.order_status = 'PAID'
group by coalesce(oi.product_category, 'Unknown')
