with 
exploded as (
    select distinct
        ltrim(rtrim(value)) as media_format_type
    from {{ ref('stg_discog_collection') }} as a
    cross apply string_split(a.MEDIA_FORMAT, ',') -- split on comma
),

final as (
    select
        row_number() over (order by b.media_format_type) as media_format_id,
        b.media_format_type
    from exploded as b
    where b.media_format_type is not null and b.media_format_type != ''
)

select * from final
