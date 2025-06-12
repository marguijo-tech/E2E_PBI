with 
staging_data as (
    select *
    from {{ ref('stg_rs_top_500') }}
),

enriched_data as (
    select
        a.ALBUM_RANKING,
        a.ARTIST_NAME,
        a.ALBUM_NAME,
        a.RECORD_LABEL_NAME,
        a.RELEASE_YEAR,
        a.RELEASE_DAY,
        a.RELEASE_MONTH,
        cast(
            concat(
                cast(a.RELEASE_YEAR as varchar), '-',
                right(concat('00', cast(a.RELEASE_MONTH as varchar)), 2), '-',
                right(concat('00', cast(a.RELEASE_DAY as varchar)), 2)
            ) as date
        ) as RELEASE_DATE,
        a.GENRE,
        a.TRACK_COUNT,
        a.RECORD_SOURCE
    from staging_data as a
)

select * from enriched_data

