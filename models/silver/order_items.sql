with cleaned as (
    select
        nullif(trim(order_item_id::text), '')::bigint as order_item_id,
        nullif(trim(order_id::text), '')::bigint as order_id,
        nullif(trim(product_id::text), '')::bigint as product_id,
        nullif(trim(product_category::text), '') as product_category,
        nullif(trim(quantity::text), '')::integer as quantity,
        nullif(trim(unit_price::text), '')::numeric(12, 2) as unit_price,
        nullif(trim(promotion_id::text), '')::bigint as promotion_id,
        nullif(trim(attributed_to_promo::text), '')::boolean as attributed_to_promo,
        row_number() over (partition by order_item_id order by order_item_id) as row_num
    from {{ source('bronze', 'order_items_raw') }}
    where nullif(trim(order_item_id::text), '') is not null
)

select
    order_item_id,
    order_id,
    product_id,
    product_category,
    quantity,
    unit_price,
    quantity * unit_price as line_total,
    promotion_id,
    attributed_to_promo
from cleaned
where row_num = 1
