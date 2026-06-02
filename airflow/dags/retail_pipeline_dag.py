from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = Path(os.getenv("RETAIL_PROJECT_DIR", "/opt/airflow/project"))
DATA_DIR = PROJECT_DIR / "data"
ENV_FILE = PROJECT_DIR / ".env"
DBT_COMMAND = f"{sys.executable} -c 'from dbt.cli.main import cli; cli()'"

CSV_FILES = [
    "customers.csv",
    "orders.csv",
    "order_items.csv",
    "promotions.csv",
]


def load_env_file() -> dict[str, str]:
    env = os.environ.copy()

    if not ENV_FILE.exists():
        return env

    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        env.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    return env


def validate_csv_files() -> None:
    missing_files = [
        str(DATA_DIR / file_name)
        for file_name in CSV_FILES
        if not (DATA_DIR / file_name).exists()
    ]

    if missing_files:
        raise FileNotFoundError(
            "Missing required CSV file(s): " + ", ".join(missing_files)
        )


task_env = load_env_file()

with DAG(
    dag_id="retail_data_platform",
    description="Load bronze CSV data, build dbt models, and run dbt tests.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail", "dbt", "postgres"],
) as dag:
    validate_sources = PythonOperator(
        task_id="validate_csv_files",
        python_callable=validate_csv_files,
    )

    inspect_raw_data = BashOperator(
        task_id="inspect_raw_data",
        bash_command=f"python {PROJECT_DIR / 'inspect_raw_data.py'}",
        cwd=str(PROJECT_DIR),
        env=task_env,
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"python {PROJECT_DIR / 'load_bronze.py'}",
        cwd=str(PROJECT_DIR),
        env=task_env,
    )

    dbt_prep = BashOperator(
        task_id="dbt_prep",
        bash_command=(
            f"{DBT_COMMAND} clean "
            f"--project-dir {PROJECT_DIR} "
            f"--profiles-dir {PROJECT_DIR} "
            f"&& {DBT_COMMAND} deps "
            f"--project-dir {PROJECT_DIR} "
            f"--profiles-dir {PROJECT_DIR}"
        ),
        cwd=str(PROJECT_DIR),
        env=task_env,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{DBT_COMMAND} run "
            f"--project-dir {PROJECT_DIR} "
            f"--profiles-dir {PROJECT_DIR}"
        ),
        cwd=str(PROJECT_DIR),
        env=task_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_COMMAND} test "
            f"--select silver gold "
            f"--project-dir {PROJECT_DIR} "
            f"--profiles-dir {PROJECT_DIR}"
        ),
        cwd=str(PROJECT_DIR),
        env=task_env,
    )

    export_outputs = BashOperator(
        task_id="export_outputs",
        bash_command=f"python {PROJECT_DIR / 'export_outputs.py'}",
        cwd=str(PROJECT_DIR),
        env=task_env,
    )

    validate_sources >> inspect_raw_data >> load_bronze >> dbt_prep >> dbt_run >> dbt_test >> export_outputs
