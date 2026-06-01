select
    dc.customer_key,
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    coalesce(dc.customer_segment, 'Unknown') as customer_segment,
    coalesce(dc.preferred_channel, 'Unknown') as preferred_channel,
    coalesce(dc.age_group, 'Unknown') as age_group,
    coalesce(dc.income_level, 'Unknown') as income_level,
    coalesce(dc.preferred_category, 'Unknown') as preferred_category,
    dc.promo_sensitivity,
    count(fo.order_id) filter (where fo.order_status = 'PAID') as paid_orders,
    count(fo.order_id) as total_orders,
    coalesce(sum(fo.order_value) filter (where fo.order_status = 'PAID'), 0) as total_revenue,
    avg(fo.order_value) filter (where fo.order_status = 'PAID') as avg_order_value,
    min(fo.order_date) filter (where fo.order_status = 'PAID') as first_order_date,
    max(fo.order_date) filter (where fo.order_status = 'PAID') as last_order_date,
    count(fo.order_id) filter (
        where fo.order_status = 'PAID'
          and fo.attributed_to_promo
    ) as promo_orders,
    coalesce(sum(fo.order_value) filter (
        where fo.order_status = 'PAID'
          and fo.attributed_to_promo
    ), 0) as promo_revenue
from {{ ref('dim_customer') }} dc
left join {{ ref('fact_orders') }} fo
    on dc.customer_key = fo.customer_key
group by
    dc.customer_key,
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    coalesce(dc.customer_segment, 'Unknown'),
    coalesce(dc.preferred_channel, 'Unknown'),
    coalesce(dc.age_group, 'Unknown'),
    coalesce(dc.income_level, 'Unknown'),
    coalesce(dc.preferred_category, 'Unknown'),
    dc.promo_sensitivity
