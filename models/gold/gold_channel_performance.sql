with order_channels as (
    select
        order_channel as channel,
        count(*) filter (where order_status = 'PAID') as total_orders,
        coalesce(sum(order_value) filter (where order_status = 'PAID'), 0) as total_revenue,
        count(*) filter (where order_status = 'PAID' and attributed_to_promo) as promo_orders,
        coalesce(sum(order_value) filter (where order_status = 'PAID' and attributed_to_promo), 0) as promo_revenue,
        count(distinct customer_id) filter (where order_status = 'PAID') as unique_customers,
        avg(order_value) filter (where order_status = 'PAID') as avg_order_value
    from {{ ref('orders') }}
    group by order_channel
),
campaign_channels as (
    select
        campaign_channel as channel,
        count(*) as total_campaigns,
        coalesce(sum(budget_allocated), 0) as total_budget_allocated
    from {{ ref('promotions') }}
    group by campaign_channel
)

select
    coalesce(oc.channel, cc.channel, 'Unknown') as channel,
    coalesce(oc.total_orders, 0) as total_orders,
    coalesce(oc.total_revenue, 0) as total_revenue,
    coalesce(oc.promo_orders, 0) as promo_orders,
    coalesce(oc.promo_revenue, 0) as promo_revenue,
    coalesce(oc.unique_customers, 0) as unique_customers,
    oc.avg_order_value,
    coalesce(cc.total_campaigns, 0) as total_campaigns,
    coalesce(cc.total_budget_allocated, 0) as total_budget_allocated
from order_channels oc
full outer join campaign_channels cc
    on oc.channel = cc.channel
