{{ config(alias='silver_orders') }}

with prepared as (
    select
        {{ text_clean('order_id') }} as order_id_txt,
        {{ text_clean('customer_id') }} as customer_id_txt,
        {{ text_clean('order_date') }} as order_date_txt,
        {{ text_clean('order_status') }} as order_status_txt,
        {{ text_clean('promotion_id') }} as promotion_id_txt,
        {{ text_clean('order_channel') }} as order_channel_txt,
        {{ text_clean('order_value') }} as order_value_txt,
        {{ text_clean('attributed_to_promo') }} as attributed_to_promo_txt,
        {{ text_clean('customer_segment_at_time') }} as customer_segment_at_time_txt
    from {{ latest_bronze('orders_raw', 'order_id') }}
),
typed as (
    select
        *,
        case when order_id_txt ~ '^\d+$' then order_id_txt::bigint end as order_id,
        case when customer_id_txt ~ '^\d+$' then customer_id_txt::bigint end as customer_id,
        {{ parse_date('order_date_txt') }} as order_date,
        case when promotion_id_txt ~ '^\d+$' then promotion_id_txt::bigint end as promotion_id,
        case when order_value_txt ~ '^-?\d+(\.\d+)?$' then order_value_txt::numeric(12, 2) end as raw_order_value,
        {{ bool_value('attributed_to_promo_txt') }} as attributed_to_promo
    from prepared
),
cleaned as (
    select
        t.order_id,
        t.customer_id,
        t.order_date,
        case {{ normalize_token('t.order_status_txt') }}
            when 'cancel' then 'CANCEL'
            when 'cancelled' then 'CANCEL'
            when 'canceled' then 'CANCEL'
            when 'error' then 'ERROR'
            when 'invalid' then 'INVALID'
            when 'paid' then 'PAID'
            when 'pending' then 'PENDING'
            else 'UNKNOWN'
        end as order_status,
        case when p.promotion_id is not null then t.promotion_id end as promotion_id,
        case {{ normalize_token('t.order_channel_txt') }}
            when 'direct' then 'Direct'
            when 'email' then 'Email'
            when 'push' then 'Push'
            when 'sms' then 'SMS'
            when 'social' then 'Social'
            else 'Unknown'
        end as order_channel,
        case when t.raw_order_value >= 0 then t.raw_order_value end as order_value,
        coalesce(t.attributed_to_promo, false) as attributed_to_promo,
        case {{ normalize_token('t.customer_segment_at_time_txt') }}
            when 'high_value' then 'High_Value'
            when 'low_value' then 'Low_Value'
            when 'medium_value' then 'Medium_Value'
            when 'new_customer' then 'New_Customer'
            else 'Unknown'
        end as customer_segment_at_time,
        (t.promotion_id_txt is null) as missing_promotion_flag,
        (coalesce(t.attributed_to_promo, false) and t.promotion_id_txt is null) as promo_attributed_without_promotion_id_flag,
        (t.raw_order_value is null or t.raw_order_value < 0) as invalid_order_value_flag,
        (t.promotion_id_txt is not null and p.promotion_id is null) as invalid_promotion_reference_flag,
        md5(concat_ws('|',
            t.customer_id_txt, t.order_date_txt, t.order_status_txt, t.promotion_id_txt,
            t.order_channel_txt, t.order_value_txt, t.attributed_to_promo_txt,
            t.customer_segment_at_time_txt
        )) as row_signature
    from typed t
    join {{ ref('customers') }} c
        on t.customer_id = c.customer_id
    left join {{ ref('promotions') }} p
        on t.promotion_id = p.promotion_id
    where t.order_id is not null
),
deduped as (
    select
        *,
        row_number() over (
            partition by order_id
            order by order_date desc nulls last, row_signature desc
        ) as row_num
    from cleaned
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
    customer_segment_at_time,
    missing_promotion_flag,
    promo_attributed_without_promotion_id_flag,
    invalid_order_value_flag,
    invalid_promotion_reference_flag
from deduped
where row_num = 1
