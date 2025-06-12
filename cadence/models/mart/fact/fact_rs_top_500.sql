with
refined_data as (
    select * from {{ ref('ref_rs_top_500') }}
)

select * from refined_data
