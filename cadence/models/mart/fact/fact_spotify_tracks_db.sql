with
refined_data as (
    select * from {{ ref('ref_spotify_tracks_db') }}
)

select * from refined_data