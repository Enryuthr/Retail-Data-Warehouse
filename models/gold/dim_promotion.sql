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
    cost_per_acquisition
from {{ ref('promotions') }}

union all

select
    -1 as promotion_key,
    -1 as promotion_id,
    'Unknown' as campaign_name,
    'Unknown' as promo_type,
    null as discount_value,
    null as min_order_value,
    null as start_date,
    null as end_date,
    null as campaign_duration_days,
    'Unknown' as target_segment,
    'Unknown' as campaign_channel,
    'Unknown' as campaign_objective,
    'Unknown' as target_category,
    null as expected_response_rate,
    null as budget_allocated,
    null as cost_per_acquisition
