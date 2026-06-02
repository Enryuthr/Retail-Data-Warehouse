# Retail Data Platform

Retail analytics pipeline using PostgreSQL, Python, and dbt.

## Layers

- `bronze`: raw CSV data loaded into PostgreSQL by `load_bronze.py`
- `silver`: cleaned tables plus dimensional star schema built by dbt
- `gold`: analytics marts built by dbt

## Project Structure

```text
data/                 CSV source files
macros/               dbt macros
models/silver/        cleaned and standardized source tables
models/gold/          star schema dimensions, facts, and analytics marts
load_bronze.py        CSV to bronze loader
dbt_project.yml       dbt project config
profiles.yml.example  dbt profile template
.env.example          environment variable template
```

## Setup

Create a virtual environment and install dependencies:

```powershell
python -m venv venv
venv\Scripts\python.exe -m pip install -r requirements.txt
```

Create `.env` from `.env.example` and fill in your PostgreSQL password.

Create `profiles.yml` from `profiles.yml.example`.

## Run Pipeline

Load CSV files into the `bronze` schema:

```powershell
venv\Scripts\python.exe load_bronze.py
```

Build silver and gold dbt models:

```powershell
venv\Scripts\dbt.exe run --profiles-dir .
venv\Scripts\dbt.exe test --profiles-dir .
```

## Run With Airflow

Airflow is configured with Docker Compose in `docker-compose.airflow.yml`.

The Compose stack includes both Airflow and a dedicated PostgreSQL database for
the retail warehouse. Airflow connects to that database using the Docker service
name `retail-postgres`.

Start Airflow:

```powershell
docker-compose -f docker-compose.airflow.yml up airflow-init
docker-compose -f docker-compose.airflow.yml up
```

Open Airflow at http://localhost:8080 and sign in with:

```text
username: admin
password: admin
```

Trigger the `retail_data_platform` DAG. It runs:

1. Validate required CSV files exist
2. Load CSV files into the `bronze` schema
3. Run dbt models
4. Run dbt tests

Your retail database is exposed on your machine at:

```text
localhost:5433
```

Use the same `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` values from
`.env` when connecting from your SQL client.

## Dashboard With Metabase

Metabase is included in the Docker Compose stack.

Start the stack:

```powershell
docker-compose -f docker-compose.airflow.yml up
```

Open Metabase at http://localhost:3000 and create your admin account.

When Metabase asks you to add a database, use:

```text
Database type: PostgreSQL
Host: retail-postgres
Port: 5432
Database name: retail_dw
Username: value from POSTGRES_USER in .env
Password: value from POSTGRES_PASSWORD in .env
Schemas: gold, silver
```

Build dashboard cards from the gold marts:

```text
gold.mart_daily_sales
gold.mart_customer_summary
gold.mart_promotion_performance
gold.mart_product_category_performance
```

## Useful Checks

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY table_schema, table_name;
```
