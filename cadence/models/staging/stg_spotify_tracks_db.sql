with
src_data as (
    select 
        genre                       as GENRE,
        artist_name                 as ARTIST_NAME,
        track_name                  as TRACK_NAME,
        track_id                    as TRACK_ID,
        popularity                  as POPULARITY_SCORE,
        acousticness                as ACOUSTICNESS,
        danceability                as DANCEABILITY,
        duration_ms                 as DURATION_MS,
        energy                      as ENERGY,
        instrumentalness            as INSTRUMENTALNESS,
        liveness                    as LIVENESS,
        loudness                    as LOUDNESS,
        speechiness                 as SPEECHINESS,
        valence                     as VALENCE,
        [key]                       as MUSICAL_KEY,
        mode                        as MUSICAL_MODE,
        tempo                       as TEMPO,
        time_signature              as TIME_SIGNATURE,

        'dbo.Spotify_Tracks'               as RECORD_SOURCE

    from {{ source('cadence', 'Spotify_Tracks') }}
)

-- TODO:ADD HASHING LOGIC USING DBT UTILS AND GENERATE_SURROGATE_KEY

select * from src_data