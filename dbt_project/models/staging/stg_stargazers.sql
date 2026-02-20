with dbt_core as (
    select * from {{ source('raw', 'raw_dbt_core') }}
),

airflow as (
    select * from {{ source('raw', 'raw_airflow') }}
),

dagster as (
    select * from {{ source('raw', 'raw_dagster') }}
),

duckdb as (
    select * from {{ source('raw', 'raw_duckdb') }}
),

dlt as (
    select * from {{ source('raw', 'raw_dlt') }}
),

unioned as (
    select * from dbt_core
    union all
    select * from airflow
    union all
    select * from dagster
    union all
    select * from duckdb
    union all
    select * from dlt
),

final as (
    select
        user_login,
        user_id,
        repo,
        cast(starred_at   as timestamptz) as starred_at,
        avatar_url,
        html_url,
        cast(extracted_at as timestamptz) as extracted_at
    from unioned
)

select * from final
