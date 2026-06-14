# Gold Dimensional Model ERD

This ERD shows the core Gold layer dimension and fact tables.

```mermaid
erDiagram
    DIM_CUSTOMER ||--o{ FACT_ORDERS : "customer_key"
    DIM_PROMOTION ||--o{ FACT_ORDERS : "promotion_key"
    DIM_DATE ||--o{ FACT_ORDERS : "order_date_key"
    FACT_ORDERS ||--o{ FACT_ORDER_ITEMS : "order_id"
    DIM_PRODUCT ||--o{ FACT_ORDER_ITEMS : "product_key"
    DIM_PROMOTION ||--o{ FACT_ORDER_ITEMS : "promotion_key"

    DIM_CUSTOMER {
        bigint customer_key PK
        bigint customer_id UK
        string first_name
        string last_name
        string email
        string customer_segment
        string preferred_channel
        string age_group
        string income_level
        string preferred_category
    }

    DIM_PROMOTION {
        bigint promotion_key PK
        bigint promotion_id
        string campaign_name
        string promo_type
        numeric discount_value
        date start_date
        date end_date
        string target_segment
        string campaign_channel
        string campaign_objective
        string target_category
    }

    DIM_PRODUCT {
        bigint product_key PK
        bigint product_id
        string product_category
    }

    DIM_DATE {
        integer date_key PK
        date full_date
        integer year
        integer quarter
        integer month
        string month_name
        integer day
        integer day_of_week
        string day_name
        boolean is_weekend
    }

    FACT_ORDERS {
        bigint order_id PK
        bigint customer_key FK
        bigint promotion_key FK
        integer order_date_key FK
        date order_date
        string order_status
        string order_channel
        numeric order_value
        boolean attributed_to_promo
        string customer_segment_at_time
    }

    FACT_ORDER_ITEMS {
        bigint order_item_id PK
        bigint order_id FK
        bigint product_key FK
        bigint promotion_key FK
        integer quantity
        numeric unit_price
        numeric line_total
        boolean attributed_to_promo
    }
```

## Relationship Summary

| From table | Column | To table | Column | Relationship |
|---|---|---|---|---|
| `fact_orders` | `customer_key` | `dim_customer` | `customer_key` | Many orders per customer |
| `fact_orders` | `promotion_key` | `dim_promotion` | `promotion_key` | Many orders per promotion |
| `fact_orders` | `order_date_key` | `dim_date` | `date_key` | Many orders per date |
| `fact_order_items` | `order_id` | `fact_orders` | `order_id` | Many items per order |
| `fact_order_items` | `product_key` | `dim_product` | `product_key` | Many order items per product |
| `fact_order_items` | `promotion_key` | `dim_promotion` | `promotion_key` | Many order items per promotion |

Note: these are logical primary and foreign keys validated by dbt tests, not physical PostgreSQL constraints.
