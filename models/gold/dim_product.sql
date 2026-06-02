select
    row_number() over (order by product_id, product_category) as product_key,
    product_id,
    coalesce(product_category, 'Unknown') as product_category
from (
    select distinct
        product_id,
        product_category
    from {{ ref('order_items') }}
    where product_id is not null
) products
