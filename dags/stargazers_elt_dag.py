"""
Airflow DAG: stargazers_elt
============================
Runs once daily:
  1. extract_load  – pulls stargazers from GitHub API → DuckDB (raw_stargazers)
  2. dbt_run       – runs dbt models (stg_stargazers, stargazer_summary)
  3. dbt_test      – runs dbt tests to validate the output

Prerequisites
-------------
* Set AIRFLOW_HOME to the project root so Airflow picks up this dags/ folder.
* Set GITHUB_TOKEN and optionally DUCKDB_PATH in your .env or environment.
* dbt must be on PATH inside the Airflow worker environment.
"""

import os
import sys
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Add project root to sys.path so PythonOperator can import extract_load/
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from extract_load.github_extract_load import run_extract_load  # noqa: E402

DBT_PROJECT_DIR = str(PROJECT_ROOT / "dbt_project")
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH", str(PROJECT_ROOT / "data" / "stargazers.duckdb")
)

# Propagate the resolved DuckDB path into BashOperator tasks
TASK_ENV = {**os.environ, "DUCKDB_PATH": DUCKDB_PATH}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="stargazers_elt",
    default_args=default_args,
    description="Daily ELT: GitHub stargazers → DuckDB → dbt",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["github", "stargazers", "elt"],
) as dag:

    extract_load_task = PythonOperator(
        task_id="extract_load",
        python_callable=run_extract_load,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
        env=TASK_ENV,
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
        env=TASK_ENV,
    )

    extract_load_task >> dbt_run_task >> dbt_test_task
