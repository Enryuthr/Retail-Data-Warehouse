select
    customer_id as customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    customer_segment,
    preferred_channel,
    age_group,
    income_level,
    avg_order_value as source_avg_order_value,
    promo_sensitivity,
    email_opt_in,
    sms_opt_in,
    push_opt_in,
    total_lifetime_orders as source_lifetime_orders,
    preferred_category
from {{ ref('customers') }}
