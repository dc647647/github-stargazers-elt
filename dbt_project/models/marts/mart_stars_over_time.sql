with daily as (
    select
        repo,
        cast(starred_at as date) as starred_date,
        count(*)                 as stars_on_day
    from {{ ref('int_stargazers') }}
    group by repo, cast(starred_at as date)
),

cumulative as (
    select
        repo,
        starred_date,
        stars_on_day,
        sum(stars_on_day) over (
            partition by repo
            order by starred_date
        ) as cumulative_stars
    from daily
)

select * from cumulative
order by repo, starred_date
