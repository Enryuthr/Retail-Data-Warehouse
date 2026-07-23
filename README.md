# Retail Data Platform Learning Project

This is a hands-on data engineering project for learning how a small retail analytics platform works end to end.

It starts with raw CSV files, loads them into PostgreSQL, cleans and models them with dbt, and runs the workflow with Airflow.

The goal is not to build a perfect production platform. The goal is to understand the moving parts of a real ELT pipeline.

## What You Learn

- How raw files become database tables
- How Bronze, Silver, and Gold data layers work
- How dbt models clean and transform data
- How dbt tests catch bad data
- How Airflow runs a pipeline step by step
- How Docker runs Airflow and Metabase alongside PostgreSQL 18 on Windows
- How Python scripts support ingestion and profiling

## Project Flow

```text
CSV files
  -> inspect raw data
  -> load Bronze tables in Postgres
  -> build Silver dbt models
  -> build Gold dbt models
  -> run dbt tests
```

## Architecture

```text
data/*.csv
   |
   v
inspect_raw_data.py
   |
   v
load_bronze.py -> PostgreSQL bronze schema
   |
   v
dbt models/silver -> cleaned tables
   |
   v
dbt models/gold -> star schema tables
```

Airflow runs those steps as one DAG.

## Main Folders

| Path               | Purpose                         |
| ------------------ | ------------------------------- |
| `data/`          | Raw input CSV files             |
| `airflow/dags/`  | Airflow pipeline definition     |
| `models/silver/` | dbt cleaning models             |
| `models/gold/`   | dbt star schema models          |
| `macros/`        | Reusable dbt SQL helpers        |
| `tests/`         | Custom dbt tests                |
| `reports/`       | Raw data profiling output       |
| `docker/`        | Postgres initialization SQL     |

## Important Files

| File                           | What It Does                                                               |
| ------------------------------ | -------------------------------------------------------------------------- |
| `docker-compose.airflow.yml` | Starts Postgres, Airflow, dbt helper, and Metabase                         |
| `pipeline_utils.py`          | Shared paths,`.env` loading, table lists, and database connection helper |
| `inspect_raw_data.py`        | Profiles raw CSV files and writes quality reports                          |
| `load_bronze.py`             | Loads raw CSV files into PostgreSQL`bronze` tables                       |
| `dbt_project.yml`            | dbt project configuration                                                  |
| `profiles.yml.example`       | Example dbt database connection config                                     |
| `models/sources.yml`         | Declares raw Bronze source tables for dbt                                  |

## Data Layers

### Bronze

Bronze is the raw landing layer.

Tables:

- `bronze.customers_raw`
- `bronze.orders_raw`
- `bronze.order_items_raw`
- `bronze.promotions_raw`

Bronze keeps the source data close to the original CSV files.

### Silver

Silver is the cleaned layer.

It handles:

- trimming text
- parsing dates
- converting numbers and booleans
- normalizing categories and statuses
- removing duplicates
- validating relationships
- adding data quality flags

Main models:

- `customers.sql`
- `orders.sql`
- `order_items.sql`
- `promotions.sql`
- `data_quality_report.sql`

### Gold

Gold is the star schema layer.

It contains:

- dimensions: `dim_customer`, `dim_product`, `dim_promotion`, `dim_date`
- facts: `fact_orders`, `fact_order_items`

Gold keeps only reusable dimensions and facts. Extra marts can be rebuilt later from this star schema.

## Run With Docker and Airflow

Build the shared Airflow image once:

```powershell
docker compose -f docker-compose.airflow.yml build
```

Start the stack:

```powershell
docker compose -f docker-compose.airflow.yml up -d
```

Rebuild only after changing a requirements file. Dependencies are installed in the image, not each time a container starts.

Open Airflow:

```text
http://localhost:18081
```

Login:

```text
admin / admin
```

Trigger the DAG:

```text
retail_data_platform
```

Local service ports:

| Service              | URL / Port                 |
| -------------------- | -------------------------- |
| Airflow              | `http://localhost:18081` |
| Metabase             | `http://localhost:13001` |
| Retail PostgreSQL 18 | `localhost:5432`         |

Stop the stack:

```powershell
docker compose -f docker-compose.airflow.yml down
```

## Run dbt Manually

```powershell
docker compose -f docker-compose.airflow.yml run --rm dbt dbt run --profiles-dir .
docker compose -f docker-compose.airflow.yml run --rm dbt dbt test --profiles-dir .
```

## Run Python Scripts Manually

```powershell
python -m venv venv
venv\Scripts\python.exe -m pip install -r requirements.txt
venv\Scripts\python.exe inspect_raw_data.py
venv\Scripts\python.exe load_bronze.py
```

## Learning Path

1. Start with `data/` and understand the raw files.
2. Read `inspect_raw_data.py` to see how the project checks raw data.
3. Read `load_bronze.py` to see how CSVs enter Postgres.
4. Read `models/silver/` to learn cleaning logic.
5. Read `models/gold/` to learn star schema modeling.
6. Open `airflow/dags/retail_pipeline_dag.py` to see how the full workflow is connected.
7. Run the DAG and inspect `reports/`.

## Common Debugging Commands

Check running containers:

```powershell
docker compose -f docker-compose.airflow.yml ps
```

Check Airflow logs:

```powershell
docker compose -f docker-compose.airflow.yml logs --tail=100 airflow-webserver
docker compose -f docker-compose.airflow.yml logs --tail=100 airflow-scheduler
```

Check dbt models:

```powershell
docker compose -f docker-compose.airflow.yml run --rm dbt dbt debug --profiles-dir .
docker compose -f docker-compose.airflow.yml run --rm dbt dbt test --profiles-dir .
```

## Notes

- `.env` is local config and should not be committed.
- `target/`, `logs/`, `dbt_packages/`, and `venv/` are generated.
- The pipeline uses PostgreSQL 18 on Windows at `localhost:5432`; Docker services reach it at `host.docker.internal:5432`.
- This project favors readable learning code over production complexity.
