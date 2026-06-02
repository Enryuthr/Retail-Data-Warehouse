# Retail Data Platform

Production-ready medallion pipeline for ecommerce promotion analytics using CSV
bronze loads, dbt silver/gold transformations, PostgreSQL, Airflow, Docker, and
CSV/Parquet exports.

## Folder Structure

```text
data/                         Input CSV files
reports/                      Raw inspection reports
outputs/silver/               Silver CSV and Parquet exports
outputs/gold/                 Gold CSV and Parquet exports
airflow/dags/                 Airflow DAG
docker/postgres/init/         Warehouse schema bootstrap SQL
macros/                       Reusable dbt cleaning macros
models/silver/                Clean silver models and quality report
models/gold/                  Dimensions, facts, marts, and requested gold tables
tests/                        Custom dbt assertions
load_bronze.py                CSV to bronze loader
inspect_raw_data.py           Raw CSV profiler
export_outputs.py             Silver/gold CSV and Parquet exporter
docker-compose.airflow.yml    Airflow, warehouse Postgres, dbt runner, Metabase
```

## Run With Airflow

```powershell
docker compose -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.airflow.yml up
```

Open Airflow at `http://localhost:8080` with `admin` / `admin`, then trigger
the `retail_data_platform` DAG. The DAG validates CSVs, profiles raw data,
loads bronze, runs dbt, runs dbt tests, and exports silver/gold outputs.

## Run dbt With Docker

```powershell
docker compose -f docker-compose.airflow.yml run --rm dbt dbt run --profiles-dir .
docker compose -f docker-compose.airflow.yml run --rm dbt dbt test --profiles-dir .
```

The `dbt` service connects to the same `retail-postgres` warehouse used by
Airflow.

## Run Locally

```powershell
python -m venv venv
venv\Scripts\python.exe -m pip install -r requirements.txt
venv\Scripts\python.exe inspect_raw_data.py
venv\Scripts\python.exe load_bronze.py
venv\Scripts\dbt.exe run --profiles-dir .
venv\Scripts\dbt.exe test --profiles-dir .
venv\Scripts\python.exe export_outputs.py
```

## Silver Tables

`silver.silver_customers`: one row per `customer_id`; standardized profile
fields, parsed registration and last purchase timestamps where valid, opt-ins as
booleans, and flags for invalid phone, missing last purchase, and invalid
registration date.

`silver.silver_orders`: one row per `order_id`; valid customer references only,
standardized status/channel, non-negative order values, valid promotion
references where present, and promotion/value quality flags.

`silver.silver_order_items`: one row per `order_item_id`; valid order references
only, standardized category, `item_revenue`, `line_total`, and quantity/unit
price/promotion attribution flags.

`silver.silver_promotions`: one row per `promotion_id`; standardized campaign
fields, parsed start/end timestamps, validated numeric campaign measures, and
date/discount/response-rate/budget flags.

`silver.data_quality_report`: issue type, table name, affected row count, and
severity for primary-key, bad-value, attribution, date, and relationship checks.

## Gold Tables

`gold.gold_customer_360`: one row per customer with profile fields, order counts,
paid/cancelled counts, promo/non-promo revenue, preferred purchased category,
recency, and lifecycle segment.

`gold.gold_promotion_performance`: one row per promotion with campaign fields,
orders/customers/revenue/items, attributed metrics, estimated CPA,
revenue-per-budget, campaign status, and promotion validity flags.

`gold.gold_channel_performance`: one row per order/campaign channel with orders,
revenue, promo metrics, unique customers, average order value, campaign count,
and budget.

`gold.gold_category_performance`: one row per product category with items sold,
revenue, unique orders/customers, and promo-attributed revenue.

## Data Quality Summary

Current `silver.data_quality_report` output:

```text
customers_raw: invalid_phone = 2000, invalid_registration_date = 2000
orders_raw: missing_promotion_id = 11695
order_items_raw: invalid_quantity = 64
promotions_raw: invalid_date_range = 35, negative_discount_value = 16
```

Rows are flagged wherever possible. Rows are excluded only when they cannot
satisfy required grain or required foreign-key constraints, such as orders with
invalid customers or order items with invalid orders. Invalid optional promotion
references are nulled and flagged so facts remain usable.

## Key Assumptions

- Valid dates start with an ISO date component like `YYYY-MM-DD`; full timestamps
  are accepted.
- Customer `registration_date` and many phone values are damaged in the sample
  CSV, so they are retained with quality flags rather than repaired.
- Paid orders drive revenue metrics in the customer, promotion, channel, and
  category gold tables.
- Missing `promotion_id` is valid for non-promo orders, but it is reported as a
  low-severity quality issue and flagged when attribution says promo-driven.
- Lifecycle segmentation uses current warehouse date: `Active` up to 30 days,
  `At Risk` up to 90 days, otherwise `Dormant`; customers with no paid order are
  `New`.
