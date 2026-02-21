select user_login, user_id, repo, starred_at, avatar_url, html_url, extracted_at
from {{ ref('stg_dbt_core') }}

union all

select user_login, user_id, repo, starred_at, avatar_url, html_url, extracted_at
from {{ ref('stg_airflow') }}

union all

select user_login, user_id, repo, starred_at, avatar_url, html_url, extracted_at
from {{ ref('stg_dagster') }}

union all

select user_login, user_id, repo, starred_at, avatar_url, html_url, extracted_at
from {{ ref('stg_duckdb') }}

union all

select user_login, user_id, repo, starred_at, avatar_url, html_url, extracted_at
from {{ ref('stg_dlt') }}
