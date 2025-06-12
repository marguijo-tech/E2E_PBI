with
staging_data as (
    select *
    from {{ ref('stg_spotify_tracks_db') }}
),

enriched_data as (
    select
        a.GENRE,
        a.ARTIST_NAME,
        a.TRACK_NAME,
        a.TRACK_ID,
        a.POPULARITY_SCORE,
        case
            when a.POPULARITY_SCORE between 80 and 100 then 'High'
            when a.POPULARITY_SCORE between 50 and 79 then 'Medium'
            when a.POPULARITY_SCORE between 0 and 49 then 'Low'
            else 'Unknown'
        end as POPULARITY_GENERAL,
        a.ACOUSTICNESS,
        a.DANCEABILITY,
        a.DURATION_MS,
        a.ENERGY,
        a.INSTRUMENTALNESS,
        a.LIVENESS,
        a.LOUDNESS,
        a.SPEECHINESS,
        a.VALENCE,
        a.MUSICAL_KEY,
        a.MUSICAL_MODE,
        a.TEMPO,
        a.TIME_SIGNATURE,
        a.RECORD_SOURCE
    from staging_data as a
)

select * from enriched_data