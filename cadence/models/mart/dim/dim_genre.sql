with distinct_genres as (
    select distinct ltrim(rtrim(a.GENRE)) as genre_name
    from {{ ref('ref_spotify_tracks_db') }} as a

    union 

    select distinct ltrim(rtrim(b.GENRE)) as genre_name
    from {{ ref('ref_rs_top_500') }} as b
)

select
    row_number() over (order by genre_name) as genre_id,
    genre_name
from distinct_genres
where genre_name is not null;