select
    customer_id as customer_key,
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
    avg_order_value as source_avg_order_value,
    promo_sensitivity,
    email_opt_in,
    sms_opt_in,
    push_opt_in,
    last_purchase_date,
    total_lifetime_orders as source_lifetime_orders,
    preferred_category
from {{ ref('customers') }}

union all

select
    -1 as customer_key,
    -1 as customer_id,
    'Unknown' as first_name,
    null as last_name,
    null as email,
    null as phone_number,
    null as registration_date,
    'Unknown' as customer_segment,
    'Unknown' as preferred_channel,
    'Unknown' as age_group,
    'Unknown' as income_level,
    null as source_avg_order_value,
    null as promo_sensitivity,
    null as email_opt_in,
    null as sms_opt_in,
    null as push_opt_in,
    null as last_purchase_date,
    null as source_lifetime_orders,
    'Unknown' as preferred_category
