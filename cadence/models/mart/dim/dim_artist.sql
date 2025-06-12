with distinct_artists as (
    select distinct ltrim(rtrim(a.MAIN_ARTIST_CLEANED)) as artist_name
    from {{ ref('ref_discog_collection') }} as a

    union 

    select distinct ltrim(rtrim(b.ARTIST_NAME)) as artist_name
    from {{ ref('ref_streaming_history_music') }} as b

    union 

    select distinct ltrim(rtrim(c.ARTIST_NAME)) as artist_name
    from {{ ref('ref_rs_top_500') }} as c

    union

    select distinct ltrim(rtrim(d.ARTIST_NAME)) as artist_name
    from {{ ref('ref_marquee') }} as d
    
)

select
    row_number() over (order by artist_name) as artist_id,
    artist_name
from distinct_artists
where artist_name is not null;