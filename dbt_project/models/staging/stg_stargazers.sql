with source as (
    select * from {{ source('raw', 'raw_stargazers') }}
),

renamed as (
    select
        user_login,
        user_id,
        repo,
        cast(starred_at   as timestamptz) as starred_at,
        avatar_url,
        html_url,
        cast(extracted_at as timestamptz) as extracted_at
    from source
)

select * from renamed
