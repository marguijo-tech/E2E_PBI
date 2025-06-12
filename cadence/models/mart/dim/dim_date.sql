with
nums as (
    select top (DATEDIFF(day, '2000-01-01', '2050-12-31') + 1)
        row_number() over (order by (select null)) - 1 as n
    from sys.all_objects a
    cross join sys.all_objects b
),

dates as (
    select dateadd(day, n, cast('2000-01-01' as date)) as date_value
    from nums
),

final as (
    select
        date_value,
        year(date_value) as year,
        month(date_value) as month,
        day(date_value) as day,
        datename(weekday, date_value) as day_name,
        datename(month, date_value) as month_name
    from dates
)

select * from final
