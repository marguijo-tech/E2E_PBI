with    
src_data as (
    
    select 
        endTime                                     as DATE_LISTENED,
        podcastName                                 as PODCAST_NAME,
        episodeName                                 as EPISODE_NAME,
        msPlayed                                    as MS_PLAYED,

        'dbo.StreamingHistory_podcast_0'               as RECORD_SOURCE

    from {{ source('cadence', 'StreamingHistory_podcast_0') }}
)

-- TODO:ADD HASHING LOGIC USING DBT UTILS AND GENERATE_SURROGATE_KEY

select * from src_data