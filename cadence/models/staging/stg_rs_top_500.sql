with
src_data as (
    select 
        ranking                     as ALBUM_RANKING,
        artist                      as ARTIST_NAME,
        title                       as ALBUM_NAME,
        label                       as RECORD_LABEL_NAME,
        year                        as RELEASE_YEAR,
        day                         as RELEASE_DAY,
        month                       as RELEASE_MONTH,
        genre                       as GENRE,
        number_of_songs             as TRACK_COUNT,

        'dbo.Rolling_Stone_Top_500_Albums'               as RECORD_SOURCE

    
    from {{ source('cadence', 'Rolling_Stone_Top_500_Albums') }}
)

-- TODO:ADD HASHING LOGIC USING DBT UTILS AND GENERATE_SURROGATE_KEY

select * from src_data