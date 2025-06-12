with
src_data as (
    select 
        artistName                  as ARTIST_NAME,
        segment                     as LISTEN_FREQUENCY,

        'dbo.Marquee'               as RECORD_SOURCE
    
    from {{ source('cadence', 'Marquee') }}
)

-- TODO:ADD HASHING LOGIC USING DBT UTILS AND GENERATE_SURROGATE_KEY

select * from src_data