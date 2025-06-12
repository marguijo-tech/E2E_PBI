with
src_data as (
    select 
        followerCount               as TOTAL_FOLLOWERS,
        followingUsersCount         as TOTAL_FOLLOWING,
        dismissingUsersCount        as TOTAL_BLOCKED,
        
        'dbo.Follow'               as RECORD_SOURCE
    
    from {{ source('cadence', 'Follow') }}
)

-- TODO:ADD HASHING LOGIC USING DBT UTILS AND GENERATE_SURROGATE_KEY

select * from src_data
