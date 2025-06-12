with
staging_date as (
    select *
    from {{ ref('stg_marquee') }}
),

enriched_data as (
    select        
        a.ARTIST_NAME,
        a.LISTEN_FREQUENCY,
        case
            when a.LISTEN_FREQUENCY in ('Previously Active Listeners', 'Light listeners') then 'Low'
            when a.LISTEN_FREQUENCY = 'Moderate listeners' then 'Medium'
            when a.LISTEN_FREQUENCY = 'Super Listeners' then 'High'
            else 'Unknown'
        end as FANDOM_LEVEL

    from staging_date as a
)

select * from enriched_data
