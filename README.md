# GitHub Stargazers ELT Pipeline

An open-source ELT pipeline that tracks who has starred a curated list of
data-engineering repos on GitHub, loads the raw data into DuckDB, and
transforms it with dbt. The entire pipeline is orchestrated by Apache Airflow
and runs once per day.

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
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   └── stg_stargazers.sql    # typed, cleaned view
│       └── marts/
│           ├── schema.yml
│           └── stargazer_summary.sql # one row per user, count of repos starred
├── data/                             # DuckDB file lives here (gitignored)
├── .env.example
├── requirements.txt
└── README.md
```

---

## Data Model

### Raw layer — `raw_stargazers`

Written directly by `extract_load/github_extract_load.py`.

| Column | Type | Description |
|--------|------|-------------|
| `user_login` | VARCHAR | GitHub username |
| `user_id` | BIGINT | Numeric GitHub user ID (stable) |
| `repo` | VARCHAR | `owner/repo` format |
| `starred_at` | TIMESTAMPTZ | When the user starred the repo |
| `avatar_url` | VARCHAR | Avatar image URL |
| `html_url` | VARCHAR | GitHub profile URL |
| `extracted_at` | TIMESTAMPTZ | When this row was extracted |

### Staging — `stg_stargazers` (view)

Light cleaning and type casting on top of `raw_stargazers`. Same shape, stricter types.

### Mart — `stargazer_summary` (table)

One row per GitHub user. The primary analytical output.

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | BIGINT | Stable GitHub user ID (primary key) |
| `user_login` | VARCHAR | GitHub username |
| `repos_starred_count` | BIGINT | Number of tracked repos starred |
| `repos_starred` | VARCHAR[] | Ordered list of repos starred (by `starred_at`) |
| `first_starred_at` | TIMESTAMPTZ | Earliest star across all tracked repos |
| `last_starred_at` | TIMESTAMPTZ | Most recent star across all tracked repos |

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
source venv/bin/activate      # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Open .env and set your GITHUB_TOKEN

# 5. Initialise Airflow (sets project root as AIRFLOW_HOME)
export AIRFLOW_HOME=$(pwd)
airflow db migrate

# 6. Start Airflow
airflow standalone
# Creates an admin user and launches the scheduler + webserver
```

Go to **http://localhost:8080**, log in with the credentials printed in the
terminal, enable the `stargazers_elt` DAG, and trigger a manual run.

---

## Running Without Airflow (development / one-off)

```bash
# Extract from GitHub and load raw data into DuckDB
python -m extract_load.github_extract_load

# Run dbt transformations
cd dbt_project
dbt run --profiles-dir .

# Run dbt tests
dbt test --profiles-dir .
```

---

## Design Decisions & Trade-offs

### Full refresh per repo (not incremental)
Each daily run deletes and re-inserts all records for a given repo before
re-fetching. This keeps the logic simple and handles both new stars **and**
removed stars correctly. The trade-off is runtime: repos like apache/airflow
(~38 k stars) require hundreds of paginated API calls. With an authenticated
token (5,000 req/hr) this completes in a few minutes per repo.

### DuckDB over Postgres
DuckDB is a zero-config, file-based analytical database. No server to spin up,
no connection strings to manage — the entire database is a single `.duckdb`
file that travels with the project.

### dbt-duckdb adapter
`dbt-duckdb` connects dbt directly to the DuckDB file, keeping the full stack
local. `profiles.yml` is committed to the repo (no secrets) and reads the
database path from the `DUCKDB_PATH` environment variable.

### `user_id` as the stable user key
GitHub usernames can change. `user_id` is the permanent numeric identifier
assigned by GitHub and is used as the primary key in `stargazer_summary`.
