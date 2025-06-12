with
refined_data as (
    select * from {{ ref('ref_discog_collection') }}
)

select * from refined_data