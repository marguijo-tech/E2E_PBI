with
refined_data as (
    select * from {{ ref('ref_marquee') }}
)

select * from refined_data