with order_metrics as (
    select
        promotion_key,
        count(distinct order_id) filter (where order_status = 'PAID') as paid_orders,
        count(distinct customer_key) filter (where order_status = 'PAID') as unique_customers,
        sum(order_value) filter (where order_status = 'PAID') as total_revenue,
        avg(order_value) filter (where order_status = 'PAID') as avg_order_value
    from {{ ref('fact_orders') }}
    where promotion_key is not null
    group by promotion_key
),
item_metrics as (
    select
        promotion_key,
        count(distinct order_item_id) as total_order_items,
        sum(line_total) as item_revenue
    from {{ ref('fact_order_items') }}
    where promotion_key is not null
    group by promotion_key
)

select
    dp.promotion_key,
    dp.promotion_id,
    coalesce(dp.campaign_name, 'Unknown') as campaign_name,
    coalesce(dp.promo_type, 'Unknown') as promo_type,
    coalesce(dp.campaign_channel, 'Unknown') as campaign_channel,
    coalesce(dp.campaign_objective, 'Unknown') as campaign_objective,
    coalesce(dp.target_segment, 'Unknown') as target_segment,
    coalesce(dp.target_category, 'Unknown') as target_category,
    dp.discount_value,
    dp.budget_allocated,
    dp.cost_per_acquisition,
    dp.start_date,
    dp.end_date,
    dp.is_valid_date_range,
    coalesce(om.paid_orders, 0) as paid_orders,
    coalesce(om.unique_customers, 0) as unique_customers,
    coalesce(om.total_revenue, 0) as total_revenue,
    om.avg_order_value,
    coalesce(im.total_order_items, 0) as total_order_items,
    coalesce(im.item_revenue, 0) as item_revenue,
    case
        when dp.budget_allocated is null or dp.budget_allocated = 0 then null
        else round(
            (coalesce(om.total_revenue, 0) - dp.budget_allocated) / dp.budget_allocated,
            4
        )
    end as revenue_roi
from {{ ref('dim_promotion') }} dp
left join order_metrics om
    on dp.promotion_key = om.promotion_key
left join item_metrics im
    on dp.promotion_key = im.promotion_key
