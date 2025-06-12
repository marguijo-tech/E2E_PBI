with
refined_data as (
    select * from {{ ref('ref_streaming_history_music') }}
)

select * from refined_data
