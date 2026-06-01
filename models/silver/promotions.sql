with cleaned as (
    select
        nullif(trim(promotion_id::text), '')::bigint as promotion_id,
        nullif(trim(campaign_name::text), '') as campaign_name,
        nullif(trim(promo_type::text), '') as promo_type,
        nullif(trim(discount_value::text), '')::numeric(12, 2) as discount_value,
        nullif(trim(min_order_value::text), '')::numeric(12, 2) as min_order_value,
        nullif(trim(start_date::text), '')::timestamp as start_date,
        nullif(trim(end_date::text), '')::timestamp as end_date,
        nullif(trim(campaign_duration_days::text), '')::integer as campaign_duration_days,
        nullif(trim(target_segment::text), '') as target_segment,
        nullif(trim(campaign_channel::text), '') as campaign_channel,
        nullif(trim(campaign_objective::text), '') as campaign_objective,
        nullif(trim(target_category::text), '') as target_category,
        nullif(trim(expected_response_rate::text), '')::numeric(8, 4) as expected_response_rate,
        nullif(trim(budget_allocated::text), '')::numeric(14, 2) as budget_allocated,
        nullif(trim(cost_per_acquisition::text), '')::numeric(12, 2) as cost_per_acquisition,
        row_number() over (partition by promotion_id order by promotion_id) as row_num
    from {{ source('bronze', 'promotions_raw') }}
    where nullif(trim(promotion_id::text), '') is not null
)

select
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
    case
        when start_date is null or end_date is null then null
        when end_date >= start_date then true
        else false
    end as is_valid_date_range
from cleaned
where row_num = 1
