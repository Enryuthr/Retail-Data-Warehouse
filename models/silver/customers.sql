{{ config(alias='silver_customers') }}

with prepared as (
    select
        {{ text_clean('customer_id') }} as customer_id_txt,
        {{ text_clean('first_name') }} as first_name_txt,
        {{ text_clean('last_name') }} as last_name_txt,
        {{ text_clean('email') }} as email_txt,
        {{ text_clean('phone_number') }} as phone_number_txt,
        {{ text_clean('registration_date') }} as registration_date_txt,
        {{ text_clean('customer_segment') }} as customer_segment_txt,
        {{ text_clean('preferred_channel') }} as preferred_channel_txt,
        {{ text_clean('age_group') }} as age_group_txt,
        {{ text_clean('income_level') }} as income_level_txt,
        {{ text_clean('avg_order_value') }} as avg_order_value_txt,
        {{ text_clean('promo_sensitivity') }} as promo_sensitivity_txt,
        {{ text_clean('email_opt_in') }} as email_opt_in_txt,
        {{ text_clean('sms_opt_in') }} as sms_opt_in_txt,
        {{ text_clean('push_opt_in') }} as push_opt_in_txt,
        {{ text_clean('last_purchase_date') }} as last_purchase_date_txt,
        {{ text_clean('total_lifetime_orders') }} as total_lifetime_orders_txt,
        {{ text_clean('preferred_category') }} as preferred_category_txt
    from {{ source('bronze', 'customers_raw') }}
),
typed as (
    select
        *,
        case when customer_id_txt ~ '^\d+$' then customer_id_txt::bigint end as customer_id,
        {{ parse_date('registration_date_txt') }} as registration_date,
        {{ parse_date('last_purchase_date_txt') }} as last_purchase_date
    from prepared
),
cleaned as (
    select
        customer_id,
        initcap(first_name_txt) as first_name,
        initcap(last_name_txt) as last_name,
        lower(email_txt) as email,
        phone_number_txt as phone_number,
        registration_date,
        case {{ normalize_token('customer_segment_txt') }}
            when 'high_value' then 'High_Value'
            when 'low_value' then 'Low_Value'
            when 'medium_value' then 'Medium_Value'
            when 'new_customer' then 'New_Customer'
            else 'Unknown'
        end as customer_segment,
        case {{ normalize_token('preferred_channel_txt') }}
            when 'direct' then 'Direct'
            when 'email' then 'Email'
            when 'push' then 'Push'
            when 'sms' then 'SMS'
            when 'social' then 'Social'
            else 'Unknown'
        end as preferred_channel,
        case regexp_replace(lower(age_group_txt), '[^a-z0-9+]+', '', 'g')
            when '1825' then '18-25'
            when '2635' then '26-35'
            when '3645' then '36-45'
            when '4655' then '46-55'
            when '55+' then '55+'
            else 'Unknown'
        end as age_group,
        case {{ normalize_token('income_level_txt') }}
            when 'high' then 'High'
            when 'low' then 'Low'
            when 'medium' then 'Medium'
            when 'premium' then 'Premium'
            else 'Unknown'
        end as income_level,
        case when avg_order_value_txt ~ '^-?\d+(\.\d+)?$' then avg_order_value_txt::numeric(12, 2) end as avg_order_value,
        case when promo_sensitivity_txt ~ '^-?\d+(\.\d+)?$' then promo_sensitivity_txt::numeric(6, 3) end as promo_sensitivity,
        {{ bool_value('email_opt_in_txt') }} as email_opt_in,
        {{ bool_value('sms_opt_in_txt') }} as sms_opt_in,
        {{ bool_value('push_opt_in_txt') }} as push_opt_in,
        last_purchase_date,
        case when total_lifetime_orders_txt ~ '^-?\d+$' then total_lifetime_orders_txt::integer end as total_lifetime_orders,
        case {{ normalize_token('preferred_category_txt') }}
            when 'beauty' then 'Beauty'
            when 'books' then 'Books'
            when 'clothing' then 'Clothing'
            when 'electronics' then 'Electronics'
            when 'home' then 'Home'
            when 'sports' then 'Sports'
            else 'Unknown'
        end as preferred_category,
        (phone_number_txt is not null and (
            phone_number_txt ~ '^-'
            or regexp_replace(phone_number_txt, '[^0-9]', '', 'g') = ''
        )) as invalid_phone_flag,
        (last_purchase_date_txt is null) as missing_last_purchase_flag,
        (registration_date_txt is not null and registration_date is null) as invalid_registration_date_flag,
        md5(concat_ws('|',
            first_name_txt, last_name_txt, email_txt, phone_number_txt, registration_date_txt,
            customer_segment_txt, preferred_channel_txt, age_group_txt, income_level_txt,
            avg_order_value_txt, promo_sensitivity_txt, email_opt_in_txt, sms_opt_in_txt,
            push_opt_in_txt, last_purchase_date_txt, total_lifetime_orders_txt, preferred_category_txt
        )) as row_signature
    from typed
    where customer_id is not null
),
deduped as (
    select
        *,
        row_number() over (partition by customer_id order by registration_date desc nulls last, row_signature desc) as row_num
    from cleaned
)

select
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    registration_date,
    customer_segment,
    preferred_channel,
    age_group,
    income_level,
    avg_order_value,
    promo_sensitivity,
    email_opt_in,
    sms_opt_in,
    push_opt_in,
    last_purchase_date,
    total_lifetime_orders,
    preferred_category,
    invalid_phone_flag,
    missing_last_purchase_flag,
    invalid_registration_date_flag
from deduped
where row_num = 1
