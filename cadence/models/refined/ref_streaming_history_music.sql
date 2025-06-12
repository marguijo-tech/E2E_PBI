with
staging_data as (
    select *
    from {{ ref('stg_streaming_history_music') }}
),

enriched_data as (
    select
        cast(a.DATE_LISTENED as date) as DATE_LISTENED,
        a.ARTIST_NAME,
        a.TRACK_NAME,
        a.MS_PLAYED,
        concat(
            floor(a.MS_PLAYED / 60000), -- Minutes
            ':',
            right('00' + cast((a.MS_PLAYED % 60000) / 1000 as varchar), 2) -- Seconds zero-padded
        ) as PLAY_DURATION
    from staging_data as a
)

select * from enriched_data
