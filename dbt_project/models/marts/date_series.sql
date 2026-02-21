with spine as (
    select unnest(
        generate_series(date '2013-01-01', current_date, interval '1 day')
    )::date as date_day
)

select
    date_day,

    -- calendar attributes
    extract(year      from date_day)::int  as year,
    extract(quarter   from date_day)::int  as quarter,
    extract(month     from date_day)::int  as month,
    monthname(date_day)                    as month_name,
    extract(week      from date_day)::int  as week_of_year,
    extract(dayofweek from date_day)::int  as day_of_week,   -- 0 = Sunday
    dayname(date_day)                      as day_name,

    -- period start dates
    date_trunc('month',   date_day)::date                                        as date_month,
    date_trunc('quarter', date_day)::date                                        as date_quarter,
    case
        when extract(month from date_day) <= 6
        then date_trunc('year', date_day)::date
        else (date_trunc('year', date_day) + interval '6 months')::date
    end                                                                          as date_half,

    -- flags
    extract(dayofweek from date_day) in (0, 6) as is_weekend,
    extract(dayofweek from date_day) not in (0, 6) as is_weekday

from spine
order by date_day
