select
    repos_starred_count,
    count(*) as user_count
from {{ ref('stargazer_summary') }}
group by repos_starred_count
order by repos_starred_count
