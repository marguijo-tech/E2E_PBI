with
src_data as (
    select 
        displayName                  as USER_NAME,
        firstName                    as FIRST_NAME,
        lastName                     as LAST_NAME,
        imageUrl                     as IMAGE_URL,
        largeImageUrl                as LARGE_IMAGE_URL,
        tasteMaker                   as TASTE_MAKER_FLAG,
        verified                     as VERIFIED_FLAG,
        
        'dbo.Identity'               as RECORD_SOURCE
    
    from {{ source('cadence', 'Identity') }}
)

-- TODO:ADD HASHING LOGIC USING DBT UTILS AND GENERATE_SURROGATE_KEY

select * from src_data