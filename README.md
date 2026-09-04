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
date-partitioned CSV batch
  -> validate and inspect source files
  -> append an idempotent Bronze batch
  -> build Silver dbt models
  -> build Gold dbt models
  -> run dbt tests
```

## Architecture

```text
data/*.csv (base snapshot)
   |
generate_dummy_data.py -> data/incoming/YYYY-MM-DD/*.csv
   |
validate -> load_bronze.py -> PostgreSQL bronze schema
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
| `data/`          | Base snapshot and generated date-partitioned input batches |
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
| `generate_dummy_data.py`     | Creates deterministic initial and daily delta batches                    |
| `load_bronze.py`             | Validates and transactionally appends one source batch to Bronze         |
| `complete_pipeline.py`       | Marks a tested batch complete and advances the pipeline watermark        |
| `reset_dev_data.py`          | Explicitly clears only this project's Bronze/control development rows    |
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

Bronze keeps the source data close to the original CSV files and retains every source version.
The original business columns remain available alongside `batch_id`, `logical_date`,
`source_file`, `file_checksum`, `source_updated_at`, `ingested_at`, `operation`, `business_date`,
`source_row_number`, and `source_record_hash`.

`control.ingestion_batches`, `control.ingested_files`, and `control.pipeline_watermarks` record
batch status, checksums, row metrics, and the last successfully completed logical date. A
Bronze load finishes with status `ingested`; `complete_pipeline.py` changes it to `completed`
only after dbt has run and passed. Therefore a failed dbt run does not move the watermark.
Completing an older backfill records that batch as complete but never moves the watermark
backwards.

## Deterministic daily batches

The tracked `data/*.csv` files are the base snapshot. Generated files are written under
`data/incoming/YYYY-MM-DD/` and are ignored by Git. The first date is a full initial snapshot;
later dates contain only inserts (`I`), updates (`U`), and tombstones (`D`). The same date and
seed produce the same bytes. A small number of new orders and their items use an earlier
`business_date` to model late arrival; change `--late-records` when needed.

```powershell
python generate_dummy_data.py --date 2026-09-01 --mode initial --seed 42
python generate_dummy_data.py --date 2026-09-02 --mode delta --seed 42 --late-records 2
python generate_dummy_data.py --date 2026-09-03 --mode delta --seed 42 --late-records 2
```

`--mode auto` selects the initial snapshot when no initial batch exists and delta mode
afterwards. Existing content is checksum-checked; use `--force` only when intentionally
regenerating a date. Generate backfill dates in order so each delta has the state expected by
the previous date.

## Incremental ingestion and backfills

Run a date manually in two phases. The watermark advances only in the last command:

```powershell
python load_bronze.py --date 2026-09-01 --pipeline-name retail_daily
docker compose -f docker-compose.airflow.yml run --rm dbt dbt run --profiles-dir .
docker compose -f docker-compose.airflow.yml run --rm dbt dbt test --profiles-dir .
python complete_pipeline.py --date 2026-09-01 --pipeline-name retail_daily
```

The loader calculates a SHA-256 checksum, validates headers, keys, operations, timestamps,
duplicate keys, and source relationships, stages rows in temporary tables, and appends them in
one PostgreSQL transaction. A repeated successful batch with the same checksums is validated and
loaded safely, then `should_run_dbt` detects the completed batch and skips the dbt rebuild and
tests. An `ingested` but unfinished batch runs dbt again for recovery. A tombstone remains in
Bronze and removes the record from the latest current-state Silver model.
Silver and Gold remain full dbt table rebuilds because they are small enough here; this keeps
late-arriving and updated relationships deterministic without making every model incremental.

If dbt model code changes without new source data, use the existing manual dbt `run` and `test`
commands above; the short-circuit only skips work for an already completed logical date.

The Airflow DAG runs every five minutes with `*/5 * * * *` while retaining the UTC logical date
`{{ ds }}`. Runs within the same UTC day therefore target the same idempotent batch and normally
short-circuit after that date is completed. It has a fixed `2026-01-01` UTC start date and
`catchup=False`, so deployment does not silently launch a large historical run. Trigger one
selected date or backfill an explicit range:

```powershell
docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags trigger retail_data_platform --exec-date 2026-09-02T00:00:00+00:00
docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags backfill retail_data_platform --start-date 2026-09-01 --end-date 2026-09-03
```

Inspect control state with:

```sql
select * from control.ingestion_batches order by logical_date;
select * from control.ingested_files order by logical_date, source_file;
select * from control.pipeline_watermarks;
```

If a task fails, fix the input or database issue and retry the same logical date. The failed
transaction rolls back Bronze rows and does not advance the watermark; a retry reuses the
batch identity and cannot duplicate a source version. To reset only local dummy data and its
generated landing batches, use the explicit guard below, then rebuild dbt models:

```powershell
python reset_dev_data.py --yes
docker compose -f docker-compose.airflow.yml run --rm dbt dbt run --profiles-dir .
```

### Silver

Silver is the cleaned layer.
It first selects the latest source version per business key using
`source_updated_at`, `ingested_at`, `batch_id`, and `source_record_hash`; tombstones are then
excluded from the current-state output.

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
| Airflow metadata PostgreSQL | `localhost:5433`    |

The retail role/database comes from `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`
(normally `retail_dw`) and contains Bronze, control, Silver, and Gold. The `airflow-postgres`
service uses its separate `airflow` role/database and is exposed as host port `5433`; it stores
Airflow metadata only, never retail business data.

Stop the stack:

```powershell
docker compose -f docker-compose.airflow.yml down
```

## Grafana Pipeline Monitoring

Grafana reads two databases directly with dedicated read-only roles:

```text
retail_dw control + silver tables ----\
                                       -> Grafana dashboard and alert rules
Airflow metadata tables --------------/
```

They remain separate because Airflow metadata describes orchestration runs, tasks, and scheduler
health, while `retail_dw` describes business-data ingestion, watermarks, and current dbt quality.
Grafana is at `http://localhost:13002`; the provisioned dashboard is **Retail Data Platform /
Retail Pipeline Monitoring**. Dashboard JSON is under `grafana/dashboards/`, and datasource,
dashboard-provider, and alert-rule definitions are under `grafana/provisioning/`.

Copy the `GRAFANA_*` keys from `.env.example` into the gitignored `.env`. The setup command below
adds only missing keys with strong local passwords, then idempotently creates or updates the two
five-connection, read-only roles. It needs the existing retail administrator values in `.env` and
the `airflow-postgres` service running:

```powershell
docker compose -f docker-compose.airflow.yml up -d airflow-postgres
powershell -ExecutionPolicy Bypass -File scripts/setup_grafana_roles.ps1
```

Start only Grafana and its required metadata database, or start the full stack:

```powershell
docker compose -f docker-compose.airflow.yml up -d grafana
docker compose -f docker-compose.airflow.yml up -d
```

Verify the service and both provisioned data sources:

```powershell
Invoke-RestMethod http://localhost:13002/api/health
$grafanaEnv = @{}
Get-Content .env | ForEach-Object {
  if ($_ -match '^([^#=]+)=(.*)$') { $grafanaEnv[$matches[1]] = $matches[2] }
}
$securePassword = ConvertTo-SecureString $grafanaEnv.GRAFANA_ADMIN_PASSWORD -AsPlainText -Force
$grafanaCredential = [pscredential]::new($grafanaEnv.GRAFANA_ADMIN_USER, $securePassword)
Invoke-RestMethod -Authentication Basic -Credential $grafanaCredential `
  http://localhost:13002/api/datasources/uid/retail-warehouse/health
Invoke-RestMethod -Authentication Basic -Credential $grafanaCredential `
  http://localhost:13002/api/datasources/uid/airflow-metadata/health
```

Batch states have distinct meanings: `started` is in progress, `ingested` means Bronze committed
but dbt/final completion may need recovery, `completed` means dbt tests passed and the watermark
was advanced, and `failed` means ingestion failed. A successful DAG run may contain neutral
`skipped` dbt tasks when `should_run_dbt` finds the logical-date batch already completed; this is
expected and is not an alert. Logical dates and stored timestamps stay in UTC, while Grafana
displays them in `Asia/Jakarta`.

The seven provisioned Grafana-managed rules are visible under **Alerting > Alert rules**. They do
not send notifications until you add a user-owned destination under **Alerting > Contact points**
and route the rules with a notification policy. No email, webhook, or messaging credential is
invented or stored by this project.

### Troubleshooting Grafana monitoring

- `host.docker.internal` failure: confirm Windows PostgreSQL listens on port 5432 and permits the
  Docker subnet; test the Retail Warehouse datasource from Grafana.
- Permission errors: rerun `scripts/setup_grafana_roles.ps1`; the Grafana roles intentionally have
  access only to the listed monitoring tables and cannot write or browse unrelated schemas.
- Provisioning YAML/JSON errors: validate the tracked files, then inspect
  `docker compose -f docker-compose.airflow.yml logs grafana`.
- Blank retail panels: run the initial ingestion plus dbt build so the control tables and
  `silver.data_quality_report` exist.
- Stale scheduler heartbeat: confirm `airflow-scheduler` is running and inspect its logs before
  silencing the alert.

`docker compose -f docker-compose.airflow.yml down` preserves Grafana's named volume.
`docker compose -f docker-compose.airflow.yml down -v` deletes named-volume data and is only for
an intentional local reset. A remote or production deployment must replace `sslmode=disable` with
TLS (preferably `verify-full`), use managed secrets and backups, and consider external Grafana
storage for high availability.

## Run dbt Manually

```powershell
docker compose -f docker-compose.airflow.yml run --rm dbt dbt run --profiles-dir .
docker compose -f docker-compose.airflow.yml run --rm dbt dbt test --profiles-dir .
```

## Run Python Scripts Manually

```powershell
python -m venv venv
venv\Scripts\python.exe -m pip install -r requirements.txt
venv\Scripts\python.exe generate_dummy_data.py --date 2026-09-01 --mode auto --seed 42
venv\Scripts\python.exe inspect_raw_data.py --date 2026-09-01
venv\Scripts\python.exe load_bronze.py --date 2026-09-01
docker compose -f docker-compose.airflow.yml run --rm dbt dbt run --profiles-dir .
docker compose -f docker-compose.airflow.yml run --rm dbt dbt test --profiles-dir .
venv\Scripts\python.exe complete_pipeline.py --date 2026-09-01
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
