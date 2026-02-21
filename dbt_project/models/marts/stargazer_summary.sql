/*
  stargazer_summary
  -----------------
  One row per GitHub user. Summarises how many (and which) repos
  from our tracked list each user has starred.
*/
with stargazers as (
    select * from {{ ref('int_stargazers') }}
),

summary as (
    select
        user_id,
        user_login,
        count(distinct repo)            as repos_starred_count,
        list(repo order by starred_at)  as repos_starred,
        min(starred_at)                 as first_starred_at,
        max(starred_at)                 as last_starred_at
    from stargazers
    group by user_id, user_login
)

select * from summary
