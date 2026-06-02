select 'gold_customer_360' as table_name, customer_id::text as record_id, total_revenue as invalid_amount
from {{ ref('gold_customer_360') }}
where total_revenue < 0
   or promo_revenue < 0
   or non_promo_revenue < 0

union all

select 'gold_promotion_performance', promotion_id::text, total_revenue
from {{ ref('gold_promotion_performance') }}
where total_revenue < 0
   or attributed_revenue < 0

union all

select 'gold_category_performance', product_category, total_revenue
from {{ ref('gold_category_performance') }}
where total_revenue < 0
   or promo_attributed_revenue < 0
