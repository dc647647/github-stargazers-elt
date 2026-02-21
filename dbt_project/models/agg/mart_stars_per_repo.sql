select
    repo,
    count(*) as total_stars
from {{ ref('dim_stargazers') }}
group by repo
order by total_stars desc
