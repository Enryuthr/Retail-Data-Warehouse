select
    oi.order_item_id,
    oi.order_id,
    dpr.product_key,
    dp.promotion_key,
    oi.quantity,
    oi.unit_price,
    oi.line_total,
    oi.attributed_to_promo
from {{ ref('order_items') }} oi
join {{ ref('fact_orders') }} fo
    on oi.order_id = fo.order_id
left join {{ ref('dim_product') }} dpr
    on oi.product_id = dpr.product_id
    and coalesce(oi.product_category, 'Unknown') = dpr.product_category
left join {{ ref('dim_promotion') }} dp
    on oi.promotion_id = dp.promotion_id
where oi.quantity > 0
  and oi.unit_price > 0
