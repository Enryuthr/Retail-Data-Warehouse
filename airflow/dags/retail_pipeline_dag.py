from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

PROJECT_DIR = Path(os.getenv("RETAIL_PROJECT_DIR", "/opt/airflow/project"))
DBT_COMMAND = "dbt"

if str(PROJECT_DIR) not in sys.path:
    sys.path.append(str(PROJECT_DIR))

from pipeline_utils import ENTITY_SPECS, check_batch_status, env_with_file  # noqa: E402


def validate_csv_files(logical_date: str) -> None:
    source_dir = PROJECT_DIR / "data" / "incoming" / logical_date
    missing_files = [
        str(source_dir / f"{entity}.csv")
        for entity in ENTITY_SPECS
        if not (source_dir / f"{entity}.csv").exists()
    ]
    if missing_files:
        raise FileNotFoundError("Missing generated source file(s): " + ", ".join(missing_files))


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
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )


task_env = env_with_file()

with DAG(
    dag_id="retail_data_platform",
    description="Generate and ingest one logical date, rebuild dbt layers, test, then advance the watermark.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "retail", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["retail", "dbt", "postgres", "incremental"],
) as dag:
    generate_daily_source = bash_task(
        "generate_daily_source",
        "python generate_dummy_data.py --date '{{ ds }}' --mode auto "
        "--seed \"${RETAIL_GENERATOR_SEED:-42}\" "
        "--late-records \"${RETAIL_LATE_RECORDS:-2}\"",
    )

    validate_sources = PythonOperator(
        task_id="validate_csv_files",
        python_callable=validate_csv_files,
        op_kwargs={"logical_date": "{{ ds }}"},
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
    )

    inspect_raw_data = bash_task(
        "inspect_raw_data",
        "python inspect_raw_data.py --date '{{ ds }}'",
    )

    load_bronze = bash_task(
        "load_bronze",
        "python load_bronze.py --date '{{ ds }}' --pipeline-name retail_daily",
    )

    should_run_dbt = ShortCircuitOperator(
        task_id="should_run_dbt",
        python_callable=check_batch_status,
        op_kwargs={"logical_date": "{{ ds }}"},
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=5),
    )

    dbt_prep = bash_task(
        "dbt_prep",
        f"{dbt_command('clean')} && {dbt_command('deps')}",
    )
    dbt_run = bash_task("dbt_run", dbt_command("run"))
    dbt_test = bash_task("dbt_test", dbt_command("test"))
    complete_pipeline = bash_task(
        "complete_pipeline",
        "python complete_pipeline.py --date '{{ ds }}' --pipeline-name retail_daily",
    )

    (
        generate_daily_source
        >> validate_sources
        >> inspect_raw_data
        >> load_bronze
        >> should_run_dbt
        >> dbt_prep
        >> dbt_run
        >> dbt_test
        >> complete_pipeline
    )
