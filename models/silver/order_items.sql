{{ config(alias='silver_order_items') }}

with prepared as (
    select
        {{ text_clean('order_item_id') }} as order_item_id_txt,
        {{ text_clean('order_id') }} as order_id_txt,
        {{ text_clean('product_id') }} as product_id_txt,
        {{ text_clean('product_category') }} as product_category_txt,
        {{ text_clean('quantity') }} as quantity_txt,
        {{ text_clean('unit_price') }} as unit_price_txt,
        {{ text_clean('promotion_id') }} as promotion_id_txt,
        {{ text_clean('attributed_to_promo') }} as attributed_to_promo_txt
    from {{ latest_bronze('order_items_raw', 'order_item_id') }}
),
typed as (
    select
        *,
        case when order_item_id_txt ~ '^\d+$' then order_item_id_txt::bigint end as order_item_id,
        case when order_id_txt ~ '^\d+$' then order_id_txt::bigint end as order_id,
        case when product_id_txt ~ '^\d+$' then product_id_txt::bigint end as product_id,
        case when quantity_txt ~ '^-?\d+$' then quantity_txt::integer end as raw_quantity,
        case when unit_price_txt ~ '^-?\d+(\.\d+)?$' then unit_price_txt::numeric(12, 2) end as raw_unit_price,
        case when promotion_id_txt ~ '^\d+$' then promotion_id_txt::bigint end as promotion_id,
        {{ bool_value('attributed_to_promo_txt') }} as attributed_to_promo
    from prepared
),
cleaned as (
    select
        t.order_item_id,
        t.order_id,
        t.product_id,
        case {{ normalize_token('t.product_category_txt') }}
            when 'beauty' then 'Beauty'
            when 'books' then 'Books'
            when 'clothing' then 'Clothing'
            when 'electronics' then 'Electronics'
            when 'home' then 'Home'
            when 'sports' then 'Sports'
            else 'Unknown'
        end as product_category,
        case when t.raw_quantity > 0 then t.raw_quantity end as quantity,
        case when t.raw_unit_price >= 0 then t.raw_unit_price end as unit_price,
        case when p.promotion_id is not null then t.promotion_id end as promotion_id,
        coalesce(t.attributed_to_promo, false) as attributed_to_promo,
        (t.raw_quantity is null or t.raw_quantity <= 0) as invalid_quantity_flag,
        (t.raw_unit_price is null or t.raw_unit_price < 0) as invalid_unit_price_flag,
        (coalesce(t.attributed_to_promo, false) and t.promotion_id_txt is null) as promo_attributed_without_promotion_id_flag,
        (t.promotion_id_txt is not null and p.promotion_id is null) as invalid_promotion_reference_flag,
        md5(concat_ws('|',
            t.order_id_txt, t.product_id_txt, t.product_category_txt, t.quantity_txt,
            t.unit_price_txt, t.promotion_id_txt, t.attributed_to_promo_txt
        )) as row_signature
    from typed t
    join {{ ref('orders') }} o
        on t.order_id = o.order_id
    left join {{ ref('promotions') }} p
        on t.promotion_id = p.promotion_id
    where t.order_item_id is not null
),
deduped as (
    select
        *,
        row_number() over (partition by order_item_id order by row_signature desc) as row_num
    from cleaned
)

select
    order_item_id,
    order_id,
    product_id,
    product_category,
    quantity,
    unit_price,
    quantity * unit_price as item_revenue,
    quantity * unit_price as line_total,
    promotion_id,
    attributed_to_promo,
    invalid_quantity_flag,
    invalid_unit_price_flag,
    promo_attributed_without_promotion_id_flag,
    invalid_promotion_reference_flag
from deduped
where row_num = 1
