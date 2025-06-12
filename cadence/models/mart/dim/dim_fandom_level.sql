with
staging_data as (
    select *
    from {{ ref('stg_marquee') }}
),

fandom_levels as (
    select distinct
        case
            when a.LISTEN_FREQUENCY in ('Previously Active Listeners', 'Light listeners') then 'Low'
            when a.LISTEN_FREQUENCY = 'Moderate listeners' then 'Medium'
            when a.LISTEN_FREQUENCY = 'Super Listeners' then 'High'
            else 'Unknown'
        end as fandom_level
    from staging_data as a
),

final as (
    select
        fandom_level,
        case fandom_level
            when 'Low' then 1
            when 'Medium' then 2
            when 'High' then 3
            when 'Unknown' then 99
        end as fandom_level_key
    from fandom_levels
)

select * from final
