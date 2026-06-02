{{ config(alias='data_quality_report') }}

with customer_raw as (
    select
        {{ text_clean('customer_id') }} as customer_id_txt,
        {{ text_clean('phone_number') }} as phone_number_txt,
        {{ text_clean('registration_date') }} as registration_date_txt,
        {{ parse_date(text_clean('registration_date')) }} as registration_date
    from {{ source('bronze', 'customers_raw') }}
),
order_raw as (
    select
        {{ text_clean('order_id') }} as order_id_txt,
        {{ text_clean('customer_id') }} as customer_id_txt,
        {{ text_clean('promotion_id') }} as promotion_id_txt,
        {{ text_clean('order_value') }} as order_value_txt,
        {{ bool_value(text_clean('attributed_to_promo')) }} as attributed_to_promo
    from {{ source('bronze', 'orders_raw') }}
),
order_item_raw as (
    select
        {{ text_clean('order_item_id') }} as order_item_id_txt,
        {{ text_clean('order_id') }} as order_id_txt,
        {{ text_clean('promotion_id') }} as promotion_id_txt,
        {{ text_clean('quantity') }} as quantity_txt,
        {{ text_clean('unit_price') }} as unit_price_txt,
        {{ bool_value(text_clean('attributed_to_promo')) }} as attributed_to_promo
    from {{ source('bronze', 'order_items_raw') }}
),
promotion_raw as (
    select
        {{ text_clean('promotion_id') }} as promotion_id_txt,
        {{ text_clean('discount_value') }} as discount_value_txt,
        {{ text_clean('expected_response_rate') }} as expected_response_rate_txt,
        {{ text_clean('budget_allocated') }} as budget_allocated_txt,
        {{ text_clean('cost_per_acquisition') }} as cost_per_acquisition_txt,
        {{ parse_date(text_clean('start_date')) }} as start_date,
        {{ parse_date(text_clean('end_date')) }} as end_date
    from {{ source('bronze', 'promotions_raw') }}
),
checks as (
    select 'duplicate_primary_key' as issue_type, 'customers_raw' as table_name, count(*) - count(distinct customer_id_txt) as affected_row_count, 'high' as severity
    from customer_raw
    where customer_id_txt is not null

    union all
    select 'invalid_phone', 'customers_raw', count(*), 'medium'
    from customer_raw
    where phone_number_txt is not null
      and (phone_number_txt ~ '^-' or regexp_replace(phone_number_txt, '[^0-9]', '', 'g') = '')

    union all
    select 'invalid_registration_date', 'customers_raw', count(*), 'medium'
    from customer_raw
    where registration_date_txt is not null and registration_date is null

    union all
    select 'duplicate_primary_key', 'orders_raw', count(*) - count(distinct order_id_txt), 'high'
    from order_raw
    where order_id_txt is not null

    union all
    select 'invalid_customer_reference', 'orders_raw', count(*), 'high'
    from order_raw o
    left join {{ ref('customers') }} c
        on o.customer_id_txt ~ '^\d+$' and o.customer_id_txt::bigint = c.customer_id
    where c.customer_id is null

    union all
    select 'missing_promotion_id', 'orders_raw', count(*), 'low'
    from order_raw
    where promotion_id_txt is null

    union all
    select 'promo_attributed_without_promotion_id', 'orders_raw', count(*), 'high'
    from order_raw
    where coalesce(attributed_to_promo, false) and promotion_id_txt is null

    union all
    select 'invalid_order_value', 'orders_raw', count(*), 'high'
    from order_raw
    where order_value_txt is null
       or case
            when order_value_txt ~ '^-?\d+(\.\d+)?$' then order_value_txt::numeric < 0
            else true
          end

    union all
    select 'invalid_promotion_reference', 'orders_raw', count(*), 'high'
    from order_raw o
    left join {{ ref('promotions') }} p
        on o.promotion_id_txt ~ '^\d+$' and o.promotion_id_txt::bigint = p.promotion_id
    where o.promotion_id_txt is not null and p.promotion_id is null

    union all
    select 'duplicate_primary_key', 'order_items_raw', count(*) - count(distinct order_item_id_txt), 'high'
    from order_item_raw
    where order_item_id_txt is not null

    union all
    select 'invalid_order_reference', 'order_items_raw', count(*), 'high'
    from order_item_raw oi
    left join {{ ref('orders') }} o
        on oi.order_id_txt ~ '^\d+$' and oi.order_id_txt::bigint = o.order_id
    where o.order_id is null

    union all
    select 'invalid_quantity', 'order_items_raw', count(*), 'high'
    from order_item_raw
    where quantity_txt is null
       or case
            when quantity_txt ~ '^-?\d+$' then quantity_txt::integer <= 0
            else true
          end

    union all
    select 'invalid_unit_price', 'order_items_raw', count(*), 'high'
    from order_item_raw
    where unit_price_txt is null
       or case
            when unit_price_txt ~ '^-?\d+(\.\d+)?$' then unit_price_txt::numeric < 0
            else true
          end

    union all
    select 'promo_attributed_without_promotion_id', 'order_items_raw', count(*), 'high'
    from order_item_raw
    where coalesce(attributed_to_promo, false) and promotion_id_txt is null

    union all
    select 'inconsistent_order_item_promotion_attribution', 'order_items_raw', count(*), 'medium'
    from {{ ref('order_items') }} oi
    join {{ ref('orders') }} o
        on oi.order_id = o.order_id
    where oi.attributed_to_promo is distinct from o.attributed_to_promo
       or oi.promotion_id is distinct from o.promotion_id

    union all
    select 'duplicate_primary_key', 'promotions_raw', count(*) - count(distinct promotion_id_txt), 'high'
    from promotion_raw
    where promotion_id_txt is not null

    union all
    select 'invalid_date_range', 'promotions_raw', count(*), 'high'
    from promotion_raw
    where start_date is null or end_date is null or end_date < start_date

    union all
    select 'negative_discount_value', 'promotions_raw', count(*), 'high'
    from promotion_raw
    where discount_value_txt is null
       or case
            when discount_value_txt ~ '^-?\d+(\.\d+)?$' then discount_value_txt::numeric < 0
            else true
          end

    union all
    select 'invalid_expected_response_rate', 'promotions_raw', count(*), 'medium'
    from promotion_raw
    where expected_response_rate_txt is null
       or case
            when expected_response_rate_txt ~ '^-?\d+(\.\d+)?$'
                then expected_response_rate_txt::numeric < 0 or expected_response_rate_txt::numeric > 1
            else true
          end

    union all
    select 'invalid_budget_or_cpa', 'promotions_raw', count(*), 'high'
    from promotion_raw
    where budget_allocated_txt is null
       or cost_per_acquisition_txt is null
       or case
            when budget_allocated_txt ~ '^-?\d+(\.\d+)?$' then budget_allocated_txt::numeric < 0
            else true
          end
       or case
            when cost_per_acquisition_txt ~ '^-?\d+(\.\d+)?$' then cost_per_acquisition_txt::numeric < 0
            else true
          end
)

select
    issue_type,
    table_name,
    affected_row_count::bigint as affected_row_count,
    severity
from checks
where affected_row_count > 0
