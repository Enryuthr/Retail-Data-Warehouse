select
    coalesce(dpr.product_category, 'Unknown') as product_category,
    count(distinct foi.order_item_id) as total_order_items,
    count(distinct foi.order_id) as total_orders,
    sum(foi.quantity) as total_quantity_sold,
    sum(foi.line_total) as total_revenue,
    avg(foi.unit_price) as avg_unit_price,
    count(distinct fo.customer_key) as unique_customers,
    count(distinct foi.order_item_id) filter (where foi.attributed_to_promo) as promo_order_items,
    coalesce(sum(foi.line_total) filter (where foi.attributed_to_promo), 0) as promo_revenue
from {{ ref('fact_order_items') }} foi
join {{ ref('fact_orders') }} fo
    on foi.order_id = fo.order_id
join {{ ref('dim_product') }} dpr
    on foi.product_key = dpr.product_key
where fo.order_status = 'PAID'
group by
    coalesce(dpr.product_category, 'Unknown')
