with cleaned as (
    select
        nullif(trim(order_id::text), '')::bigint as order_id,
        nullif(trim(customer_id::text), '')::bigint as customer_id,
        nullif(trim(order_date::text), '')::timestamp as order_date,
        nullif(trim(order_status::text), '') as order_status,
        nullif(trim(promotion_id::text), '')::bigint as promotion_id,
        nullif(trim(order_channel::text), '') as order_channel,
        nullif(trim(order_value::text), '')::numeric(12, 2) as order_value,
        nullif(trim(attributed_to_promo::text), '')::boolean as attributed_to_promo,
        nullif(trim(customer_segment_at_time::text), '') as customer_segment_at_time,
        row_number() over (partition by order_id order by order_date desc) as row_num
    from {{ source('bronze', 'orders_raw') }}
    where nullif(trim(order_id::text), '') is not null
)

select
    order_id,
    customer_id,
    order_date,
    order_status,
    promotion_id,
    order_channel,
    order_value,
    attributed_to_promo,
    customer_segment_at_time
from cleaned
where row_num = 1
