select
    promotion_id as promotion_key,
    promotion_id,
    campaign_name,
    promo_type,
    discount_value,
    min_order_value,
    start_date,
    end_date,
    campaign_duration_days,
    target_segment,
    campaign_channel,
    campaign_objective,
    target_category,
    expected_response_rate,
    budget_allocated,
    cost_per_acquisition,
    is_valid_date_range
from {{ ref('promotions') }}
