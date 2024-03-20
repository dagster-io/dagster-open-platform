with sessions as (
    select * from {{ ref('int_sessions_aggregated') }}
),

user_stitching as (
    select * from {{ ref('int_users_stitched') }}
),

attributed as (

    select

        *,

        case
            -- Ads
            when first_gclid is not null then 'cpc'
            when first_reddit_cid is not null then 'cpc'
            when first_campaign_medium = 'cpc' then 'cpc'

            -- UTM Params
            when first_campaign_medium is not null then first_campaign_medium

            -- Search Breakdown
            when first_referrer_medium = 'search' and first_path = '/' then 'search-brand'
            when first_referrer_medium = 'search' and first_path = '/cloud' then 'search-brand'
            when first_referrer_medium = 'search' and first_path like '/blog%' then 'search-blog'
            when first_referrer_medium = 'search' and first_path like '/vs-%' then 'search-vs'
            when
                first_referrer_medium = 'search' and first_path like '/integrations%'
                then 'search-integrations'
            when first_referrer_medium = 'search' then 'search-other'

            -- Referrers
            when first_referrer_medium is not null then first_referrer_medium

            -- Path Attribution
            when first_referrer_host = 'docs.dagster.io' then 'docs'
            when first_path like '/blog%' and first_referrer_host is null then 'blog'
            when first_path = '/glossary' and first_referrer_host is null then 'glossary'
            when first_path like '/events%' and first_referrer_medium is null then 'events'

            -- Other, uncategorized
            when
                coalesce(
                    first_campaign_source,
                    first_campaign_medium,
                    first_campaign_name,
                    first_utm_term
                ) is not null
                then 'other-campaign'
            when first_referrer_host in ('dagster.io', 'dagster.cloud') then 'dagster-referrer'
            when first_referrer_host is not null then 'other-referrer'
            else 'uncategorized'
        end as attribution_category

    from sessions
),

final as (

    select

        attributed.*,
        coalesce(user_stitching.user_id, attributed.anonymous_id) as blended_user_id

    from attributed
    left join user_stitching using (anonymous_id)
)

select * from final
