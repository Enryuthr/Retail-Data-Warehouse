{{ config(alias='silver_promotions') }}

with prepared as (
    select
        {{ text_clean('promotion_id') }} as promotion_id_txt,
        {{ text_clean('campaign_name') }} as campaign_name_txt,
        {{ text_clean('promo_type') }} as promo_type_txt,
        {{ text_clean('discount_value') }} as discount_value_txt,
        {{ text_clean('min_order_value') }} as min_order_value_txt,
        {{ text_clean('start_date') }} as start_date_txt,
        {{ text_clean('end_date') }} as end_date_txt,
        {{ text_clean('campaign_duration_days') }} as campaign_duration_days_txt,
        {{ text_clean('target_segment') }} as target_segment_txt,
        {{ text_clean('campaign_channel') }} as campaign_channel_txt,
        {{ text_clean('campaign_objective') }} as campaign_objective_txt,
        {{ text_clean('target_category') }} as target_category_txt,
        {{ text_clean('expected_response_rate') }} as expected_response_rate_txt,
        {{ text_clean('budget_allocated') }} as budget_allocated_txt,
        {{ text_clean('cost_per_acquisition') }} as cost_per_acquisition_txt
    from {{ latest_bronze('promotions_raw', 'promotion_id') }}
),
typed as (
    select
        *,
        case when promotion_id_txt ~ '^\d+$' then promotion_id_txt::bigint end as promotion_id,
        case when discount_value_txt ~ '^-?\d+(\.\d+)?$' then discount_value_txt::numeric(12, 2) end as raw_discount_value,
        case when min_order_value_txt ~ '^-?\d+(\.\d+)?$' then min_order_value_txt::numeric(12, 2) end as raw_min_order_value,
        {{ parse_date('start_date_txt') }} as start_date,
        {{ parse_date('end_date_txt') }} as end_date,
        case when campaign_duration_days_txt ~ '^-?\d+$' then campaign_duration_days_txt::integer end as raw_campaign_duration_days,
        case when expected_response_rate_txt ~ '^-?\d+(\.\d+)?$' then expected_response_rate_txt::numeric(8, 4) end as raw_expected_response_rate,
        case when budget_allocated_txt ~ '^-?\d+(\.\d+)?$' then budget_allocated_txt::numeric(14, 2) end as raw_budget_allocated,
        case when cost_per_acquisition_txt ~ '^-?\d+(\.\d+)?$' then cost_per_acquisition_txt::numeric(12, 2) end as raw_cost_per_acquisition
    from prepared
),
cleaned as (
    select
        promotion_id,
        campaign_name_txt as campaign_name,
        case {{ normalize_token('promo_type_txt') }}
            when 'bogo' then 'BOGO'
            when 'fixed_amount' then 'Fixed_Amount'
            when 'free_shipping' then 'Free_Shipping'
            when 'percentage_discount' then 'Percentage_Discount'
            else 'Unknown'
        end as promo_type,
        case when raw_discount_value >= 0 then raw_discount_value end as discount_value,
        case when raw_min_order_value >= 0 then raw_min_order_value end as min_order_value,
        start_date,
        end_date,
        case when raw_campaign_duration_days >= 0 then raw_campaign_duration_days end as campaign_duration_days,
        case {{ normalize_token('target_segment_txt') }}
            when 'all_customers' then 'All_Customers'
            when 'high_value' then 'High_Value'
            when 'lapsed' then 'Lapsed'
            when 'low_value' then 'Low_Value'
            when 'medium_value' then 'Medium_Value'
            when 'new_customer' then 'New_Customer'
            else 'Unknown'
        end as target_segment,
        case {{ normalize_token('campaign_channel_txt') }}
            when 'email' then 'Email'
            when 'in_app' then 'In_App'
            when 'push' then 'Push'
            when 'sms' then 'SMS'
            when 'social' then 'Social'
            when 'website_banner' then 'Website_Banner'
            else 'Unknown'
        end as campaign_channel,
        case {{ normalize_token('campaign_objective_txt') }}
            when 'acquisition' then 'Acquisition'
            when 'cross_sell' then 'Cross_Sell'
            when 'reactivation' then 'Reactivation'
            when 'retention' then 'Retention'
            when 'upsell' then 'Upsell'
            else 'Unknown'
        end as campaign_objective,
        case {{ normalize_token('target_category_txt') }}
            when 'beauty' then 'Beauty'
            when 'books' then 'Books'
            when 'clothing' then 'Clothing'
            when 'electronics' then 'Electronics'
            when 'home' then 'Home'
            when 'sports' then 'Sports'
            else 'Unknown'
        end as target_category,
        case when raw_expected_response_rate between 0 and 1 then raw_expected_response_rate end as expected_response_rate,
        case when raw_budget_allocated >= 0 then raw_budget_allocated end as budget_allocated,
        case when raw_cost_per_acquisition >= 0 then raw_cost_per_acquisition end as cost_per_acquisition,
        (start_date is null or end_date is null or end_date < start_date) as invalid_date_range_flag,
        (raw_discount_value is null or raw_discount_value < 0) as negative_discount_flag,
        (raw_expected_response_rate is null or raw_expected_response_rate < 0 or raw_expected_response_rate > 1) as invalid_expected_response_rate_flag,
        (raw_budget_allocated is null or raw_budget_allocated < 0 or raw_cost_per_acquisition is null or raw_cost_per_acquisition < 0) as invalid_budget_flag,
        md5(concat_ws('|',
            campaign_name_txt, promo_type_txt, discount_value_txt, min_order_value_txt,
            start_date_txt, end_date_txt, campaign_duration_days_txt, target_segment_txt,
            campaign_channel_txt, campaign_objective_txt, target_category_txt,
            expected_response_rate_txt, budget_allocated_txt, cost_per_acquisition_txt
        )) as row_signature
    from typed
    where promotion_id is not null
),
deduped as (
    select
        *,
        row_number() over (partition by promotion_id order by start_date desc nulls last, row_signature desc) as row_num
    from cleaned
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
    invalid_date_range_flag,
    negative_discount_flag,
    invalid_expected_response_rate_flag,
    invalid_budget_flag,
    not invalid_date_range_flag as is_valid_date_range
from deduped
where row_num = 1
