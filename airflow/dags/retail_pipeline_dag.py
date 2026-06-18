from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = Path(os.getenv("RETAIL_PROJECT_DIR", "/opt/airflow/project"))
DBT_COMMAND = f"{sys.executable} -c 'from dbt.cli.main import cli; cli()'"

if str(PROJECT_DIR) not in sys.path:
    sys.path.append(str(PROJECT_DIR))

from pipeline_utils import CSV_FILES, env_with_file


def validate_csv_files() -> None:
    missing_files = [str(file_path) for file_path in CSV_FILES.values() if not file_path.exists()]

    if missing_files:
        raise FileNotFoundError(
            "Missing required CSV file(s): " + ", ".join(missing_files)
        )


def dbt_command(command: str, *args: str) -> str:
    return " ".join([
        DBT_COMMAND,
        command,
        *args,
        f"--project-dir {PROJECT_DIR}",
        f"--profiles-dir {PROJECT_DIR}",
    ])


def bash_task(task_id: str, command: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        cwd=str(PROJECT_DIR),
        env=task_env,
    )


task_env = env_with_file()

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

    inspect_raw_data = bash_task(
        "inspect_raw_data",
        f"python {PROJECT_DIR / 'inspect_raw_data.py'}",
    )

    load_bronze = bash_task(
        "load_bronze",
        f"python {PROJECT_DIR / 'load_bronze.py'}",
    )

    dbt_prep = bash_task(
        "dbt_prep",
        f"{dbt_command('clean')} && {dbt_command('deps')}",
    )

    dbt_run = bash_task("dbt_run", dbt_command("run"))

    dbt_test = bash_task(
        "dbt_test",
        dbt_command("test", "--select silver gold"),
    )

    export_outputs = bash_task(
        "export_outputs",
        f"python {PROJECT_DIR / 'export_outputs.py'}",
    )

    validate_sources >> inspect_raw_data >> load_bronze >> dbt_prep >> dbt_run >> dbt_test >> export_outputs
