with
src_data_combined as (
    select * from {{ source('cadence', 'StreamingHistory_music_0') }}
    UNION ALL
    select * from {{ source('cadence', 'StreamingHistory_music_1') }}
),    
    
src_data as (
    
    select 
        endTime                                    as DATE_LISTENED,
        artistName                                 as ARTIST_NAME,
        trackName                                  as TRACK_NAME,
        msPlayed                                   as MS_PLAYED,

        'dbo.StreamingHistory_music_0 + 1'               as RECORD_SOURCE

    from src_data_combined
)

-- TODO:ADD HASHING LOGIC USING DBT UTILS AND GENERATE_SURROGATE_KEY

select * from src_data