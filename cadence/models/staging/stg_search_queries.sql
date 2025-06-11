with
src_data as (
    select 
        platform                     as PLATFORM,
        searchTime                   as SEARCH_TIME,
        searchQuery                  as SEARCH_QUERY,
        searchInteractionURIs        as SEARCH_INTERACTION_URIS,


        'dbo.SearchQueries'               as RECORD_SOURCE

    
    from {{ source('cadence', 'SearchQueries') }}
)

-- TODO:ADD HASHING LOGIC USING DBT UTILS AND GENERATE_SURROGATE_KEY

select * from src_data