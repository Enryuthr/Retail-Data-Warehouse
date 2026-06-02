with order_metrics as (
    select
        promotion_id,
        count(distinct order_id) filter (where order_status = 'PAID') as total_orders,
        count(distinct customer_id) filter (where order_status = 'PAID') as total_customers,
        coalesce(sum(order_value) filter (where order_status = 'PAID'), 0) as total_revenue,
        avg(order_value) filter (where order_status = 'PAID') as avg_order_value,
        count(distinct order_id) filter (where order_status = 'PAID' and attributed_to_promo) as attributed_orders,
        coalesce(sum(order_value) filter (where order_status = 'PAID' and attributed_to_promo), 0) as attributed_revenue
    from {{ ref('orders') }}
    where promotion_id is not null
    group by promotion_id
),
item_metrics as (
    select
        promotion_id,
        coalesce(sum(quantity), 0) as total_items_sold
    from {{ ref('order_items') }}
    where promotion_id is not null
    group by promotion_id
)

select
    p.promotion_id,
    p.campaign_name,
    p.promo_type,
    p.discount_value,
    p.min_order_value,
    p.start_date,
    p.end_date,
    p.campaign_duration_days,
    p.target_segment,
    p.campaign_channel,
    p.campaign_objective,
    p.target_category,
    p.expected_response_rate,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.total_customers, 0) as total_customers,
    coalesce(o.total_revenue, 0) as total_revenue,
    coalesce(i.total_items_sold, 0) as total_items_sold,
    o.avg_order_value,
    coalesce(o.attributed_orders, 0) as attributed_orders,
    coalesce(o.attributed_revenue, 0) as attributed_revenue,
    p.budget_allocated,
    p.cost_per_acquisition,
    case
        when coalesce(o.total_customers, 0) = 0 then null
        else round(p.budget_allocated / o.total_customers, 2)
    end as estimated_cpa,
    case
        when p.budget_allocated is null or p.budget_allocated = 0 then null
        else round(coalesce(o.total_revenue, 0) / p.budget_allocated, 4)
    end as revenue_per_budget,
    case
        when p.start_date is null or p.end_date is null then 'Unknown'
        when current_date < p.start_date::date then 'Upcoming'
        when current_date between p.start_date::date and p.end_date::date then 'Active'
        else 'Completed'
    end as campaign_status,
    p.invalid_date_range_flag,
    p.negative_discount_flag,
    p.invalid_expected_response_rate_flag,
    p.invalid_budget_flag
from {{ ref('promotions') }} p
left join order_metrics o
    on p.promotion_id = o.promotion_id
left join item_metrics i
    on p.promotion_id = i.promotion_id
