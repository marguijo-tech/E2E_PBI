with
src_data as (
    select
        [Catalog#]                          as CATALOG_ID,
        Artist                              as ARTIST,
        Title                               as RECORD_TITLE,
        Label                               as RECORD_LABEL,
        Format                              as MEDIA_FORMAT,
        Rating                              as RATING,
        Released                            as RELEASE_YEAR,
        release_id                          as RELEASE_ID,
        CollectionFolder                    as COLLECTION_FOLDER,
        [Date Added]                        as DATE_ADDED_TO_COLLECTION,
        [Collection Media Condition]        as COLLECTION_MEDIA_CONDITION,
        [Collection Sleeve Condition]       as COLLECTION_SLEEVE_CONDITION,
        [Collection Notes]                  as COLLECTION_NOTES,

        'dbo_seed.Discog_Collection_Data'   as RECORD_SOURCE

    from {{ ref('Discog_Collection_Data') }}
)

-- TODO:ADD HASHING LOGIC USING DBT UTILS AND GENERATE_SURROGATE_KEY

select * from src_data
