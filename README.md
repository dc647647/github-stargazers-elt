# GitHub Stargazers ELT Pipeline

An open-source ELT pipeline that tracks who has starred a curated list of
data-engineering repos on GitHub, loads the raw data into DuckDB, and
transforms it with dbt. The entire pipeline is orchestrated by Apache Airflow
and runs once per day. A Streamlit dashboard provides interactive analytics
at both the repo and individual user level.

---

## Tracked Repos

| Repo | Description |
|------|-------------|
| [dbt-labs/dbt-core](https://github.com/dbt-labs/dbt-core) | Transform data inside your warehouse |
| [apache/airflow](https://github.com/apache/airflow) | Schedule and orchestrate data pipelines |
| [dagster-io/dagster](https://github.com/dagster-io/dagster) | Asset-based orchestration |
| [duckdb/duckdb](https://github.com/duckdb/duckdb) | Fast in-process analytics database |
| [dlt-hub/dlt](https://github.com/dlt-hub/dlt) | Python-native data load tool |

---

## Project Structure

```
github-stargazers-elt/
├── dags/
│   └── stargazers_elt_dag.py         # Airflow DAG (daily schedule)
├── extract_load/
│   └── github_extract_load.py        # GitHub API → DuckDB (raw layer)
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml                  # DuckDB adapter config
│   └── models/
│       ├── staging/                  # One view per raw table, explicit columns
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   ├── stg_dbt_core.sql
│       │   ├── stg_airflow.sql
│       │   ├── stg_dagster.sql
│       │   ├── stg_duckdb.sql
│       │   └── stg_dlt.sql
│       ├── intermediate/             # Union of all staging models
│       │   └── int_stargazers.sql
│       ├── marts/                    # Core dimension tables
│       │   ├── dim_stargazers.sql    # All star events (pass-through of int_stargazers)
│       │   └── date_series.sql       # Calendar table with date attributes and flags
│       └── agg/                      # Metric and analytical models
│           ├── stargazer_summary.sql
│           ├── agg_stars_per_repo.sql
│           ├── agg_stars_over_time.sql
│           ├── agg_stargazer_overlap.sql
│           └── agg_user_activity.sql
├── streamlit_app.py                  # BI dashboard (repo + user analytics)
├── data/                             # DuckDB file lives here (gitignored)
├── .env.example
├── requirements.txt
└── README.md
```

---

## Data Model

### Raw layer — one table per repo

Written directly by `extract_load/github_extract_load.py`. Each repo gets its
own table (`raw_dbt_core`, `raw_airflow`, `raw_dagster`, `raw_duckdb`, `raw_dlt`)
so parallel Airflow tasks can write without contention.

| Column | Type | Description |
|--------|------|-------------|
| `user_login` | VARCHAR | GitHub username |
| `user_id` | BIGINT | Numeric GitHub user ID (stable) |
| `repo` | VARCHAR | `owner/repo` format |
| `starred_at` | TIMESTAMPTZ | When the user starred the repo |
| `avatar_url` | VARCHAR | Avatar image URL |
| `html_url` | VARCHAR | GitHub profile URL |
| `extracted_at` | TIMESTAMPTZ | When this row was extracted |

### Staging — `stg_*` (views)

One view per raw table with explicit column selection and type casting. No `SELECT *`.

### Intermediate — `int_stargazers` (view)

Union of all five staging models — one row per `(user, repo)` star event across
all tracked repos.

### Marts — `dim_stargazers`, `date_series` (tables)

| Model | Description |
|-------|-------------|
| `dim_stargazers` | Materialized pass-through of `int_stargazers` — the base table for all agg models |
| `date_series` | Calendar table from 2013-01-01 to today with `year`, `quarter`, `month`, `week_of_year`, `day_name`, `is_weekend`, `is_weekday`, `date_month`, `date_quarter`, `date_half` |

### Agg — analytical models (tables)

| Model | Description |
|-------|-------------|
| `stargazer_summary` | All star events — pass-through of `dim_stargazers` |
| `agg_stars_per_repo` | Total star count per repo |
| `agg_stars_over_time` | Daily and cumulative star counts per repo, date-spined so every day has a row |
| `agg_stargazer_overlap` | Number of users grouped by how many tracked repos they starred |
| `agg_user_activity` | Per-user stats: repos starred, first/last star date, avg days between stars |

---

## Prerequisites

- Python 3.11+
- A GitHub personal access token — without one you are limited to 60 API
  requests/hour, which is insufficient for repos with tens of thousands of stars.
  Generate a token (no special scopes needed for public repos) at
  https://github.com/settings/tokens

---

## Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd github-stargazers-elt

# 2. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Open .env and set GITHUB_TOKEN (and optionally per-repo tokens)

# 5. Initialise Airflow
export AIRFLOW_HOME=$(pwd)
airflow db migrate

# 6. Start Airflow
export no_proxy='*'          # required on macOS to prevent SIGSEGV on task execution
export AIRFLOW_HOME=$(pwd)   # must be set in every new terminal session
airflow standalone
# Credentials are printed in the terminal output and saved to
# simple_auth_manager_passwords.json.generated
```

Go to **http://localhost:8080**, log in, enable the `stargazers_elt` DAG, and
trigger a manual run.

---

## Running Without Airflow (development / one-off)

```bash
# Extract and load a single repo
python -m extract_load.github_extract_load "dbt-labs/dbt-core"

# Extract and load all repos
python -m extract_load.github_extract_load

# Run dbt transformations
dbt run --project-dir dbt_project --profiles-dir dbt_project

# Run dbt tests
dbt test --project-dir dbt_project --profiles-dir dbt_project

# Launch the Streamlit dashboard
streamlit run streamlit_app.py
```

---

## Design Decisions & Trade-offs

### Per-repo raw tables
Each repo is extracted into its own DuckDB table (`raw_dbt_core`, `raw_airflow`,
etc.) so five parallel Airflow tasks can run simultaneously without write
contention. dbt then unions them in the `int_stargazers` intermediate model.

### Full refresh per repo (not incremental)
Each daily run drops and re-inserts all records for a given repo. This keeps
the logic simple and correctly handles both new stars and removed stars. The
trade-off is runtime — repos like apache/airflow (~40k stars) require hundreds
of paginated API calls, capped by GitHub at 40,000 records (400 pages × 100).
With an authenticated token (5,000 req/hr) the full pipeline completes in ~4
minutes with all five repos running in parallel.

### Per-repo GitHub tokens
Each repo can use a separate GitHub token (`GITHUB_TOKEN_DBT_CORE`, etc.),
giving each its own 5,000 req/hr rate-limit budget. Falls back to
`GITHUB_TOKEN` if a repo-specific token is not set.

### Date spine in `agg_stars_over_time`
The `date_series` calendar table is cross-joined with each repo's date range so
every day has a row — zero-filled for `stars_on_day`, with `cumulative_stars`
carrying forward correctly as a step function rather than a linearly
interpolated diagonal.

### DuckDB over Postgres
DuckDB is a zero-config, file-based analytical database. No server to spin up,
no connection strings to manage — the entire database is a single `.duckdb`
file that travels with the project.

### `user_id` as the stable user key
GitHub usernames can change. `user_id` is the permanent numeric identifier
assigned by GitHub and is used as the primary key throughout.
