with date_bounds as (
    select
        min(order_date::date) as min_date,
        max(order_date::date) as max_date
    from {{ ref('orders') }}
),
date_series as (
    select generate_series(min_date, max_date, interval '1 day')::date as full_date
    from date_bounds
)

select
    to_char(full_date, 'YYYYMMDD')::integer as date_key,
    full_date,
    extract(year from full_date)::integer as year,
    extract(quarter from full_date)::integer as quarter,
    extract(month from full_date)::integer as month,
    trim(to_char(full_date, 'Month')) as month_name,
    extract(day from full_date)::integer as day,
    extract(isodow from full_date)::integer as day_of_week,
    trim(to_char(full_date, 'Day')) as day_name,
    case
        when extract(isodow from full_date) in (6, 7) then true
        else false
    end as is_weekend
from date_series

union all

select
    -1 as date_key,
    null::date as full_date,
    null::integer as year,
    null::integer as quarter,
    null::integer as month,
    'Unknown' as month_name,
    null::integer as day,
    null::integer as day_of_week,
    'Unknown' as day_name,
    null::boolean as is_weekend
