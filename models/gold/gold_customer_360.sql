with orders_by_customer as (
    select
        customer_id,
        count(*) as total_orders,
        count(*) filter (where order_status = 'PAID') as paid_orders,
        count(*) filter (where order_status = 'CANCEL') as cancelled_orders,
        coalesce(sum(order_value) filter (where order_status = 'PAID'), 0) as total_revenue,
        avg(order_value) filter (where order_status = 'PAID') as avg_order_value_actual,
        min(order_date) filter (where order_status = 'PAID') as first_order_date,
        max(order_date) filter (where order_status = 'PAID') as last_order_date,
        count(*) filter (where order_status = 'PAID' and attributed_to_promo) as promo_orders,
        count(*) filter (where order_status = 'PAID' and not attributed_to_promo) as non_promo_orders,
        coalesce(sum(order_value) filter (where order_status = 'PAID' and attributed_to_promo), 0) as promo_revenue,
        coalesce(sum(order_value) filter (where order_status = 'PAID' and not attributed_to_promo), 0) as non_promo_revenue
    from {{ ref('orders') }}
    group by customer_id
),
category_rank as (
    select
        o.customer_id,
        oi.product_category,
        row_number() over (
            partition by o.customer_id
            order by sum(coalesce(oi.quantity, 0)) desc, oi.product_category
        ) as category_rank
    from {{ ref('orders') }} o
    join {{ ref('order_items') }} oi
        on o.order_id = oi.order_id
    where o.order_status = 'PAID'
    group by o.customer_id, oi.product_category
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone_number,
    c.registration_date,
    c.customer_segment,
    c.preferred_channel,
    c.age_group,
    c.income_level,
    c.avg_order_value as source_avg_order_value,
    c.promo_sensitivity,
    c.email_opt_in,
    c.sms_opt_in,
    c.push_opt_in,
    c.last_purchase_date,
    c.total_lifetime_orders,
    c.preferred_category,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.paid_orders, 0) as paid_orders,
    coalesce(o.cancelled_orders, 0) as cancelled_orders,
    coalesce(o.total_revenue, 0) as total_revenue,
    o.avg_order_value_actual,
    o.first_order_date,
    o.last_order_date,
    coalesce(o.promo_orders, 0) as promo_orders,
    coalesce(o.non_promo_orders, 0) as non_promo_orders,
    coalesce(o.promo_revenue, 0) as promo_revenue,
    coalesce(o.non_promo_revenue, 0) as non_promo_revenue,
    coalesce(cr.product_category, c.preferred_category, 'Unknown') as preferred_product_category,
    case
        when o.last_order_date is null then null
        else current_date - o.last_order_date::date
    end as customer_recency_days,
    case
        when o.first_order_date is null then 'New'
        when current_date - o.last_order_date::date <= 30 then 'Active'
        when current_date - o.last_order_date::date <= 90 then 'At Risk'
        else 'Dormant'
    end as lifecycle_segment,
    c.invalid_phone_flag,
    c.missing_last_purchase_flag,
    c.invalid_registration_date_flag
from {{ ref('customers') }} c
left join orders_by_customer o
    on c.customer_id = o.customer_id
left join category_rank cr
    on c.customer_id = cr.customer_id
   and cr.category_rank = 1
