with
staging_data as (
    select *
    from {{ ref('stg_discog_collection') }}
),

enriched_data as (
    select
        a.CATALOG_ID,

        -- Get first artist before comma
        ltrim(rtrim(
            case
                when charindex(',', a.ARTIST) > 0 then left(a.ARTIST, charindex(',', a.ARTIST) - 1)
                else a.ARTIST
            end
        )) as MAIN_ARTIST,

        -- Remove trailing space + (number) at end of MAIN_ARTIST
        ltrim(rtrim(
            case
                when patindex('% ([0-9])', ltrim(rtrim(
                    case
                        when charindex(',', a.ARTIST) > 0 then left(a.ARTIST, charindex(',', a.ARTIST) - 1)
                        else a.ARTIST
                    end
                ))) > 0 then
                    left(
                        ltrim(rtrim(
                            case
                                when charindex(',', a.ARTIST) > 0 then left(a.ARTIST, charindex(',', a.ARTIST) - 1)
                                else a.ARTIST
                            end
                        )),
                        patindex('% ([0-9])', ltrim(rtrim(
                            case
                                when charindex(',', a.ARTIST) > 0 then left(a.ARTIST, charindex(',', a.ARTIST) - 1)
                                else a.ARTIST
                            end
                        ))) - 1
                    )
                else
                    ltrim(rtrim(
                        case
                            when charindex(',', a.ARTIST) > 0 then left(a.ARTIST, charindex(',', a.ARTIST) - 1)
                            else a.ARTIST
                        end
                    ))
            end
        )) as MAIN_ARTIST_CLEANED,

        a.RECORD_TITLE,

        -- Get first record label before comma
        ltrim(rtrim(
            case
                when charindex(',', a.RECORD_LABEL) > 0 then left(a.RECORD_LABEL, charindex(',', a.RECORD_LABEL) - 1)
                else a.RECORD_LABEL
            end
        )) as MAIN_RECORD_LABEL,

        -- Remove parentheses/brackets and content from record label
        ltrim(rtrim(
            replace(
                replace(
                    replace(
                        case
                            when charindex(',', a.RECORD_LABEL) > 0 then left(a.RECORD_LABEL, charindex(',', a.RECORD_LABEL) - 1)
                            else a.RECORD_LABEL
                        end
                    , '(', '')
                , ')', '')
            , '[', '')
        )) as MAIN_RECORD_LABEL_CLEANED,

        a.MEDIA_FORMAT,
        a.RATING,
        a.RELEASE_YEAR,
        a.RELEASE_ID,
        CAST(a.DATE_ADDED_TO_COLLECTION AS DATE) AS DATE_ADDED_TO_COLLECTION,
        a.COLLECTION_FOLDER,
        a.COLLECTION_MEDIA_CONDITION,
        a.COLLECTION_SLEEVE_CONDITION,
        a.COLLECTION_NOTES,
        a.RECORD_SOURCE
    from staging_data as a
)

select * from enriched_data
