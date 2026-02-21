"""
Airflow DAG: stargazers_elt
============================
Runs once daily:
  1. extract_load__{repo}  – one BashOperator per repo (fresh subprocess, avoids
                             macOS fork+network issues with PythonOperator)
  2. dbt_build             – builds dbt models (stg, int, dim, agg)

Prerequisites
-------------
* Set AIRFLOW_HOME to the project root so Airflow picks up this dags/ folder.
* Set GITHUB_TOKEN and per-repo tokens in your .env or environment.
* dbt must be on PATH inside the Airflow worker environment.
"""

import os
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parent.parent

REPOS = [
    "dbt-labs/dbt-core",
    "apache/airflow",
    "dagster-io/dagster",
    "duckdb/duckdb",
    "dlt-hub/dlt",
]

DBT_PROJECT_DIR = str(PROJECT_ROOT / "dbt_project")
PYTHON = str(PROJECT_ROOT / "venv" / "bin" / "python")
DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH", str(PROJECT_ROOT / "data" / "stargazers.duckdb")
)

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

    # BashOperator spawns a fresh subprocess — avoids macOS fork+network hangs
    extract_tasks = [
        BashOperator(
            task_id=f"extract_load__{repo.replace('/', '__').replace('-', '_')}",
            bash_command=f"{PYTHON} -m extract_load.github_extract_load '{repo}'",
            env=TASK_ENV,
            cwd=str(PROJECT_ROOT),
        )
        for repo in REPOS
    ]

    dbt_build_task = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --profiles-dir .",
        env=TASK_ENV,
    )

    extract_tasks >> dbt_build_task
