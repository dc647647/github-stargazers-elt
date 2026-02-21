with daily_raw as (
    select
        repo,
        cast(starred_at as date) as starred_date,
        count(*)                 as stars_on_day
    from {{ ref('dim_stargazers') }}
    group by repo, cast(starred_at as date)
),

repo_bounds as (
    select
        repo,
        cast(min(starred_at) as date) as first_star_date
    from {{ ref('dim_stargazers') }}
    group by repo
),

-- one row per (repo, day) from the repo's first star to today
date_repo_spine as (
    select
        r.repo,
        d.date_day        as starred_date,
        d.is_weekend,
        d.is_weekday,
        d.day_name
    from repo_bounds r
    cross join {{ ref('date_series') }} d
    where d.date_day >= r.first_star_date
),

filled as (
    select
        s.repo,
        s.starred_date,
        s.is_weekend,
        s.is_weekday,
        s.day_name,
        coalesce(dr.stars_on_day, 0) as stars_on_day
    from date_repo_spine s
    left join daily_raw dr
        on dr.repo = s.repo
       and dr.starred_date = s.starred_date
)

select
    repo,
    starred_date,
    day_name,
    is_weekend,
    is_weekday,
    stars_on_day,
    sum(stars_on_day) over (
        partition by repo
        order by starred_date
    ) as cumulative_stars
from filled
order by repo, starred_date
