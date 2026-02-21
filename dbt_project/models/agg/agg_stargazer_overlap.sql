with user_repo_counts as (
    select
        user_id,
        count(distinct repo) as repos_starred_count
    from {{ ref('dim_stargazers') }}
    group by user_id
)

select
    repos_starred_count,
    count(*) as user_count
from user_repo_counts
group by repos_starred_count
order by repos_starred_count
