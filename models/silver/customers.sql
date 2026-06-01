with cleaned as (
    select
        nullif(trim(customer_id::text), '')::bigint as customer_id,
        initcap(nullif(trim(first_name::text), '')) as first_name,
        initcap(nullif(trim(last_name::text), '')) as last_name,
        lower(nullif(trim(email::text), '')) as email,
        nullif(trim(phone_number::text), '') as phone_number,
        nullif(trim(customer_segment::text), '') as customer_segment,
        nullif(trim(preferred_channel::text), '') as preferred_channel,
        nullif(trim(age_group::text), '') as age_group,
        nullif(trim(income_level::text), '') as income_level,
        nullif(trim(avg_order_value::text), '')::numeric(12, 2) as avg_order_value,
        nullif(trim(promo_sensitivity::text), '')::numeric(5, 3) as promo_sensitivity,
        nullif(trim(email_opt_in::text), '')::boolean as email_opt_in,
        nullif(trim(sms_opt_in::text), '')::boolean as sms_opt_in,
        nullif(trim(push_opt_in::text), '')::boolean as push_opt_in,
        nullif(trim(total_lifetime_orders::text), '')::integer as total_lifetime_orders,
        nullif(trim(preferred_category::text), '') as preferred_category,
        row_number() over (partition by customer_id order by customer_id) as row_num
    from {{ source('bronze', 'customers_raw') }}
    where nullif(trim(customer_id::text), '') is not null
)

select
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    customer_segment,
    preferred_channel,
    age_group,
    income_level,
    avg_order_value,
    promo_sensitivity,
    email_opt_in,
    sms_opt_in,
    push_opt_in,
    total_lifetime_orders,
    preferred_category
from cleaned
where row_num = 1
