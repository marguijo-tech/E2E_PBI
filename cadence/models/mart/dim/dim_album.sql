with distinct_albums as (
    select distinct ltrim(rtrim(a.RECORD_TITLE)) as album_name
    from {{ ref('ref_discog_collection') }} as a

    union 

    select distinct ltrim(rtrim(c.ALBUM_NAME)) as album_name
    from {{ ref('ref_rs_top_500') }} as c
)

select
    row_number() over (order by album_name) as album_id,
    album_name
from distinct_albums
where album_name is not null;
