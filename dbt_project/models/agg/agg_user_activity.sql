with star_events as (
    select
        user_login,
        user_id,
        repo,
        starred_at,
        lag(starred_at) over (
            partition by user_id
            order by starred_at
        ) as prev_starred_at
    from {{ ref('dim_stargazers') }}
),

user_stats as (
    select
        user_login,
        user_id,
        count(distinct repo)                                        as repos_starred_count,
        string_agg(distinct repo, ', ' order by repo)              as repos_starred,
        min(starred_at)                                             as first_starred_at,
        max(starred_at)                                             as last_starred_at,
        avg(
            case
                when prev_starred_at is not null
                then extract(epoch from (starred_at - prev_starred_at)) / 86400.0
            end
        )                                                           as avg_days_between_stars
    from star_events
    group by user_login, user_id
)

select * from user_stats
order by repos_starred_count desc, first_starred_at
