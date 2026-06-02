with products as (
    select distinct
        product_id,
        coalesce(product_category, 'Unknown') as product_category
    from {{ ref('order_items') }}
    where product_id is not null
)

select
    (
        'x' || substr(md5(product_id::text || '|' || product_category), 1, 15)
    )::bit(60)::bigint as product_key,
    product_id,
    product_category
from products

union all

select
    -1 as product_key,
    -1 as product_id,
    'Unknown' as product_category
